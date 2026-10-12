#include "schemeshard_audit_log.h"
#include "schemeshard_impl.h"
#include "schemeshard_import.h"
#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_xxport__tx_base.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TImport::TTxCancel: public TSchemeShard::TXxport::TTxBase {
    TEvImport::TEvCancelImportRequest::TPtr Request;

    explicit TTxCancel(TSelf *self, TEvImport::TEvCancelImportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        auto respond = [this, &request](Ydb::StatusIds::StatusCode status) -> bool {
            auto response = MakeHolder<TEvImport::TEvCancelImportResponse>(request.GetTxId());
            auto& proto = *response->Record.MutableResponse();

            proto.SetStatus(status);
            Send(Request->Sender, std::move(response), 0, Request->Cookie);

            return true;
        };

        auto it = Self->Imports.find(request.GetRequest().GetId());
        if (it == Self->Imports.end() || !IsSameDomain(it->second, request.GetDatabaseName())) {
            return respond(Ydb::StatusIds::NOT_FOUND);
        }

        TImportInfo::TPtr importInfo = it->second;
        NIceDb::TNiceDb db(txc.DB);

        switch (importInfo->State) {
        case TImportInfo::EState::Done:
        case TImportInfo::EState::Cancelled:
        case TImportInfo::EState::Cancellation:
            return respond(Ydb::StatusIds::SUCCESS);

        case TImportInfo::EState::DownloadExportMetadata:
        case TImportInfo::EState::Waiting:
            importInfo->Issue = "Cancelled manually";
            importInfo->State = TImportInfo::EState::Cancelled;

            if (importInfo->SchemaMappingGetter) {
                const auto actor = importInfo->SchemaMappingGetter;
                importInfo->SchemaMappingGetter = {};
                Send(actor, new TEvents::TEvPoisonPill());
                Self->RunningImportSchemeGetters.erase(actor);
            }

            for (ui32 itemIdx : xrange(importInfo->Items.size())) {
                auto& item = importInfo->Items.at(itemIdx);

                if (item.SchemeGetter) {
                    const auto actor = item.SchemeGetter;
                    item.SchemeGetter = {};
                    Send(actor, new TEvents::TEvPoisonPill());
                    Self->RunningImportSchemeGetters.erase(actor);
                }
                if (item.SchemeQueryExecutor) {
                    const auto actor = item.SchemeQueryExecutor;
                    item.SchemeQueryExecutor = {};
                    Send(actor, new TEvents::TEvPoisonPill());
                    Self->RunningImportSchemeQueryExecutors.erase(actor);
                }

                switch (item.State) {
                case TImportInfo::EState::CreateSchemeObject:
                case TImportInfo::EState::CreateChangefeed:
                    // Generic scheme transactions are not cancellable. Keep
                    // the import non-terminal until their proposal/result and
                    // completion notification have been observed, otherwise a
                    // following Forget could remove the durable ownership
                    // fence while DDL is still in flight.
                    if (item.WaitTxId != InvalidTxId
                        || item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                case TImportInfo::EState::Transferring:
                    if (item.WaitTxId != InvalidTxId) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                        Send(Self->SelfId(), CancelRestoreTableDataPropose(*importInfo, item.WaitTxId), 0, importInfo->Id);
                    } else if (item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                case TImportInfo::EState::BuildIndexes:
                    if (item.WaitTxId != InvalidTxId) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                        Send(Self->SelfId(), CancelIndexBuildPropose(Self, *importInfo, item.WaitTxId), 0, importInfo->Id);
                    } else if (item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                default:
                    break;
                }
            }

            if (importInfo->State == TImportInfo::EState::Cancelled) {
                importInfo->EndTime = TAppData::TimeProvider->Now();
            }

            Self->PersistImportState(db, *importInfo);
            Self->EraseEncryptionKey(db, *importInfo);
            SendNotificationsIfFinished(importInfo);

            if (importInfo->IsFinished()) {
                AuditLogImportEnd(*importInfo.Get(), Self);
            }

            return respond(Ydb::StatusIds::SUCCESS);

        default:
            return respond(Ydb::StatusIds::UNDETERMINED);
        }
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancel

struct TSchemeShard::TImport::TTxCancelAck: public TSchemeShard::TXxport::TTxBase {
    const ui64 ImportId;
    const TTxId TxId;

    explicit TTxCancelAck(TSelf* self, ui64 importId, TTxId txId)
        : TXxport::TTxBase(self)
        , ImportId(importId)
        , TxId(txId)
    {
    }

    explicit TTxCancelAck(TSelf* self, TEvSchemeShard::TEvCancelTxResult::TPtr& ev)
        : TTxCancelAck(self, ev->Cookie, TTxId(ev->Get()->Record.GetTargetTxId()))
    {
    }

    explicit TTxCancelAck(TSelf* self, TEvIndexBuilder::TEvCancelResponse::TPtr& ev)
        : TTxCancelAck(self, ev->Cookie, TTxId(ev->Get()->Record.GetTxId()))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_IMPORT_ACK;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        if (!Self->Imports.contains(ImportId)) {
            return true;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(ImportId);
        NIceDb::TNiceDb db(txc.DB);

        if (importInfo->State != TImportInfo::EState::Cancellation) {
            return true;
        }

        bool found = false;
        ui32 itemIdx;

        for (ui32 i : xrange(importInfo->Items.size())) {
            auto& item = importInfo->Items.at(i);

            if (item.State != TImportInfo::EState::Transferring && item.State != TImportInfo::EState::BuildIndexes) {
                continue;
            }

            if (item.WaitTxId == TxId) {
                found = true;

                item.State = TImportInfo::EState::Cancelled;
                item.SubState = TImportInfo::TItem::ESubState::AllocateTxId;
                item.WaitTxId = InvalidTxId;
                itemIdx = i;
            }
        }

        if (!found) {
            return true;
        }

        Self->TxIdToImport.erase(TxId);
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        if (importInfo->HasPendingCancellationWork()) {
            return true;
        }

        importInfo->State = TImportInfo::EState::Cancelled;
        importInfo->EndTime = TAppData::TimeProvider->Now();
        Self->PersistImportState(db, *importInfo);
        Self->EraseEncryptionKey(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancelAck

ITransaction* TSchemeShard::CreateTxCancelImport(TEvImport::TEvCancelImportRequest::TPtr& ev) {
    return new TImport::TTxCancel(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelImportAck(TEvSchemeShard::TEvCancelTxResult::TPtr& ev) {
    return new TImport::TTxCancelAck(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelImportAck(TEvIndexBuilder::TEvCancelResponse::TPtr& ev) {
    return new TImport::TTxCancelAck(this, ev);
}

} // NSchemeShard
} // NKikimr

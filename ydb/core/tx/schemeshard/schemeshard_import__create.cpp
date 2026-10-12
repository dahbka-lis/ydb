#include "schemeshard_audit_log.h"
#include "schemeshard_impl.h"
#include "schemeshard_import.h"
#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_import_getters.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_import_scheme_query_executor.h"
#include "schemeshard_import_table_scheme.h"
#include "schemeshard_xxport__helpers.h"
#include "schemeshard_xxport__tx_base.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/table_index.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_table_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/schemeshard_path_describer.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/core/backup/common/feature_flags.h>

#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

#include <concepts>
#include <string_view>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IMPORT

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

void TSchemeShard::EraseEncryptionKey(NIceDb::TNiceDb& db, TImportInfo& importInfo) {
    if (importInfo.EraseEncryptionKey()) {
        PersistImportSettings(db, importInfo);
    }
}

namespace {

using TItem = TImportInfo::TItem;
using EState = TImportInfo::EState;

template <typename T>
concept HasIndexPopulationMode = requires(const T& t) {
    { t.index_population_mode() } -> std::same_as<Ydb::Import::ImportFromS3Settings::IndexPopulationMode>;
};

bool PrepareNextBuildableIndex(const TImportInfo& importInfo, ui32 itemIdx, TItem& item) {
    if (!NeedToBuildIndexes(importInfo, itemIdx) || !item.Table) {
        return false;
    }

    if (item.UsesSqlSchema()) {
        const auto& indexes = item.TableSchemaPlan->GetIndexes();
        while (item.NextIndexIdx < indexes.size()) {
            if (IsLocalIndexType(indexes.Get(item.NextIndexIdx).GetType())) {
                ++item.NextIndexIdx;
                continue;
            }
            break;
        }
        return item.NextIndexIdx < indexes.size();
    }

    while (item.NextIndexIdx < item.Table->indexes_size()
        && NTableIndex::IsLocalTableIndex(item.Table->indexes(item.NextIndexIdx).type_case())) {
        ++item.NextIndexIdx;
    }
    return item.NextIndexIdx < item.Table->indexes_size();
}

THashMap<EState, int> CountItemsByState(const TVector<TItem>& items) {
    THashMap<EState, int> counter;
    for (const auto& item : items) {
        counter[item.State]++;
    }
    return counter;
}

bool AllDone(const THashMap<EState, int>& stateCounts) {
    return AllOf(stateCounts, [](const auto& stateCount) { return stateCount.first == EState::Done; });
}

bool AllDoneOrWaiting(const THashMap<EState, int>& stateCounts) {
    return AllOf(stateCounts, [](const auto& stateCount) {
        return stateCount.first == EState::Done
            || stateCount.first == EState::Waiting;
    });
}

int CountWaitingSchemeObjects(const TVector<TItem>& items) {
    return static_cast<int>(CountIf(items, [](const TItem& item) {
        return item.State == EState::Waiting
            && (item.UsesSqlSchema() || item.IsLegacyQueryObject());
    }));
}

// Legacy query-created objects are not tables. SQL tables have their own
// durable statement plan and must continue through data restore.
bool IsCreatedByQuery(const TItem& item) {
    return item.IsLegacyQueryObject();
}

TString WithTableFallbackContext(const TItem& item, TString error) {
    if (item.TableSchemaPlan
        && item.TableSchemaPlan->GetSource()
            == NKikimrImport::TTableSchemaImportPlan::SCHEMA_SOURCE_PROTOBUF
        && item.TableSchemaPlan->GetFallbackReason()) {
        return TStringBuilder()
            << "table SQL candidate failed: "
            << item.TableSchemaPlan->GetFallbackReason()
            << "; protobuf candidate failed: " << error;
    }
    return error;
}

using TTableSchemaPlan = NKikimrImport::TTableSchemaImportPlan;

TMaybe<ui32> FindSqlStatement(
    const TTableSchemaPlan& plan,
    TTableSchemaPlan::EPhase phase,
    ui32 begin)
{
    for (ui32 index = begin; index < static_cast<ui32>(plan.StatementsSize()); ++index) {
        if (plan.GetStatements(index).GetPhase() == phase) {
            return index;
        }
    }
    return Nothing();
}

bool IsSqlBaseCreate(const TItem& item) {
    if (!item.UsesSqlSchema() || !item.TableSchemaPlan
        || item.TableSchemaPlan->GetStatementCursor() >= static_cast<ui32>(item.TableSchemaPlan->StatementsSize())) {
        return false;
    }
    return item.TableSchemaPlan->GetStatements(item.TableSchemaPlan->GetStatementCursor()).GetKind()
        == TTableSchemaPlan::STATEMENT_KIND_CREATE_TABLE;
}

TMaybe<TVector<TString>> SqlIndexImplTableNames(
    const NKikimrSchemeOp::TIndexCreationConfig& index)
{
    if (IsLocalIndexType(index.GetType())) {
        return TVector<TString>{};
    }
    switch (index.GetType()) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance:
        case NKikimrSchemeOp::EIndexTypeGlobalJson:
        case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact: {
            const TVector<TString> keyColumns(
                index.GetKeyColumnNames().begin(), index.GetKeyColumnNames().end());
            const auto implTables = NTableIndex::GetImplTables(index.GetType(), keyColumns);
            TVector<TString> result;
            result.reserve(implTables.size());
            for (const std::string_view implTable : implTables) {
                result.emplace_back(implTable.data(), implTable.size());
            }
            return result;
        }
        default:
            return Nothing();
    }
}

bool CanFallbackAfterPreparationFailure(Ydb::StatusIds::StatusCode status) {
    return IsIn({
        Ydb::StatusIds::BAD_REQUEST,
        Ydb::StatusIds::PRECONDITION_FAILED,
        Ydb::StatusIds::UNSUPPORTED,
    }, status);
}

bool CanFallbackAfterProposalFailure(NKikimrScheme::EStatus status) {
    return IsIn({
        NKikimrScheme::StatusInvalidParameter,
        NKikimrScheme::StatusPreconditionFailed,
    }, status);
}

bool ShouldRetrySqlBaseProposal(NKikimrScheme::EStatus status) {
    return IsIn({
        NKikimrScheme::StatusSchemeError,
        NKikimrScheme::StatusPathDoesNotExist,
    }, status);
}

bool IsCreateViewQuery(const TString& query) {
    return query.Contains("CREATE VIEW");
}

bool IsCreateReplicationQuery(const TString& query) {
    return query.Contains("CREATE ASYNC REPLICATION");
}

bool IsCreateTransferQuery(const TString& query) {
    return query.Contains("CREATE TRANSFER");
}

bool IsCreateExternalDataSourceQuery(const TString& query) {
    return query.Contains("CREATE EXTERNAL DATA SOURCE");
}

bool IsCreateExternalTableQuery(const TString& query) {
    return query.Contains("CREATE EXTERNAL TABLE");
}

bool RewriteCreateQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (IsCreateViewQuery(query)) {
        return NYdb::NDump::RewriteCreateViewQuery(query, dbRestoreRoot, true, dbPath, issues);
    } else if (IsCreateReplicationQuery(query)) {
        return NYdb::NDump::RewriteCreateAsyncReplicationQuery(query, dbRestoreRoot, dbPath, issues);
    } else if (IsCreateTransferQuery(query)) {
        return NYdb::NDump::RewriteCreateTransferQuery(query, dbRestoreRoot, dbPath, issues);
    } else if (IsCreateExternalDataSourceQuery(query)) {
        return NYdb::NDump::RewriteCreateExternalDataSourceQuery(query, dbRestoreRoot, dbPath, issues);
    } else if (IsCreateExternalTableQuery(query)) {
        return NYdb::NDump::RewriteCreateExternalTableQuery(query, dbRestoreRoot, dbPath, issues);
    }

    issues.AddIssue(TStringBuilder() << "unsupported create query: " << query);
    return false;
}

TString GetDatabase(TSchemeShard& ss) {
    return CanonizePath(ss.RootPathElements);
}

bool ShouldDelayQueryExecutionOnError(
    const NKikimrSchemeOp::TModifyScheme& preparedQuery,
    NKikimrScheme::EStatus status)
{
    switch (preparedQuery.GetOperationType()) {
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer:
            return status == NKikimrScheme::StatusNotAvailable;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable:
            return IsIn({
                NKikimrScheme::StatusPathDoesNotExist,
                NKikimrScheme::StatusSchemeError,
            }, status);
        default:
            return false;
    }
}

}

bool ValidateImportDstPath(const TString& dstPath, TSchemeShard* ss, TString& explain) {
    const TPath path = TPath::Resolve(dstPath, ss);
    TPath::TChecker checks = path.Check();
    checks
        .IsAtLocalSchemeShard()
        .HasResolvedPrefix()
        .FailOnRestrictedCreateInTempZone();

    if (path.IsResolved()) {
        if (path->IsSysView()) {
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting();
        } else {
            checks
                .IsResolved()
                .IsDeleted();
        }
    } else {
        checks
            .NotEmpty()
            .NotResolved();
    }

    if (checks) {
        checks
            //NOTE: using TSystemUsers::Metadata() here allows restoration of paths with system-reserved names/prefixes.
            // Reason is, that these names aren't being created anew but were legitimate elsewhere or
            // at some other point in time, blocking them now would create unnecessary problems.
            .IsValidLeafName(&NACLib::TSystemUsers::Metadata())
            .DepthLimit()
            .PathsLimit();

        if (path.Parent().IsResolved()) {
            checks.DirChildrenLimit();
        }
    }

    if (!checks) {
        explain = checks.GetError();
        return false;
    }
    return true;
}

struct TSchemeShard::TImport::TTxCreate: public TSchemeShard::TXxport::TTxBase {
    TEvImport::TEvCreateImportRequest::TPtr Request;
    bool Progress;

    explicit TTxCreate(TSelf* self, TEvImport::TEvCreateImportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
        , Progress(false)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        YDB_LOG_DEBUG("TImport::TTxCreate: DoExecute");
        YDB_LOG_TRACE("Message",
            {"message", request.ShortDebugString()},
        );

        auto response = MakeHolder<TEvImport::TEvCreateImportResponse>(request.GetTxId());

        const ui64 id = request.GetTxId();
        if (Self->Imports.contains(id)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::ALREADY_EXISTS,
                TStringBuilder() << "Import with id '" << id << "' already exists"
            );
        }

        const TString& uid = GetUid(request.GetRequest().GetOperationParams());
        if (uid) {
            if (auto it = Self->ImportsByUid.find(uid); it != Self->ImportsByUid.end()) {
                if (IsSameDomain(it->second, request.GetDatabaseName())) {
                    Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), *it->second);
                    return Reply(std::move(response));
                } else {
                    return Reply(
                        std::move(response),
                        Ydb::StatusIds::ALREADY_EXISTS,
                        TStringBuilder() << "Import with uid '" << uid << "' already exists"
                    );
                }
            }
        }

        const TPath domainPath = TPath::Resolve(request.GetDatabaseName(), Self);
        {
            TPath::TChecker checks = domainPath.Check();
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory();

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, checks.GetError());
            }

            if (!request.HasUserSID() || !Self->SystemBackupSIDs.contains(request.GetUserSID())) {
                checks.ImportsLimit();
            }

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::PRECONDITION_FAILED, checks.GetError());
            }
        }

        TImportInfo::TPtr importInfo = nullptr;
        TImportInfo::EState initialState = TImportInfo::EState::Waiting;

        auto validateIndexPopulationMode = [&]<typename TSettings>(const TSettings& settings) -> bool {
            if constexpr (HasIndexPopulationMode<TSettings>) {
                if (!AppData()->FeatureFlags.GetEnableIndexMaterialization()) {
                    switch (settings.index_population_mode()) {
                    case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT:
                    case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO:
                        return Reply(
                            std::move(response),
                            Ydb::StatusIds::PRECONDITION_FAILED,
                            "Index materialization is not enabled"
                        );
                    default:
                        break;
                    }
                }
            }
            return false;
        };

        switch (request.GetRequest().GetSettingsCase()) {
        case NKikimrImport::TCreateImportRequest::kImportFromS3Settings:
            {
                auto settings = request.GetRequest().GetImportFromS3Settings();
                if (!settings.scheme()) {
                    settings.set_scheme(Ydb::Import::ImportFromS3Settings::HTTPS);
                }

                if (!settings.source_prefix().empty() && NBackup::IsExportFilteringEnabled(*AppData())) {
                    initialState = TImportInfo::EState::DownloadExportMetadata;
                }

                if (validateIndexPopulationMode(settings)) {
                    return true;
                }

                importInfo = new TImportInfo(id, uid, TImportInfo::EKind::S3, settings, domainPath.Base()->PathId, request.GetPeerName());

                if (request.HasUserSID()) {
                    importInfo->UserSID = request.GetUserSID();
                }

                TString explain;
                if (!FillItems(*importInfo, settings, explain)) {
                    return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, explain);
                }
            }
            break;

        case NKikimrImport::TCreateImportRequest::kImportFromFsSettings:
            {
                if (!AppData()->FeatureFlags.GetEnableFsBackups()) {
                    return Reply(std::move(response), Ydb::StatusIds::UNSUPPORTED, "The feature flag \"EnableFsBackups\" is disabled. The operation cannot be performed.");
                }

                if (NBackup::IsExportFilteringEnabled(*AppData())) {
                    initialState = TImportInfo::EState::DownloadExportMetadata;
                }

                const auto& settings = request.GetRequest().GetImportFromFsSettings();

                if (validateIndexPopulationMode(settings)) {
                    return true;
                }

                importInfo = new TImportInfo(id, uid, TImportInfo::EKind::FS, settings, domainPath.Base()->PathId, request.GetPeerName());

                if (request.HasUserSID()) {
                    importInfo->UserSID = request.GetUserSID();
                }

                TString explain;
                if (!FillItems(*importInfo, settings, explain)) {
                    return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, explain);
                }
            }
            break;

        default:
            Y_DEBUG_ABORT("Unknown import kind");
        }

        Y_ABORT_UNLESS(importInfo != nullptr);

        importInfo->SanitizedToken = request.GetSanitizedToken();

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistCreateImport(db, *importInfo);

        importInfo->State = initialState;
        importInfo->StartTime = TAppData::TimeProvider->Now();
        Self->PersistImportState(db, *importInfo);

        Self->AddImport(importInfo);
        Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), *importInfo);

        Progress = true;
        return Reply(std::move(response));
    }

    void DoComplete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TImport::TTxCreate: DoComplete");

        if (Progress) {
            const ui64 id = Request->Get()->Record.GetTxId();
            Self->Execute(Self->CreateTxProgressImport(id), ctx);
        }
    }

private:
    bool Reply(
        THolder<TEvImport::TEvCreateImportResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString()
    ) {
        YDB_LOG_DEBUG("TImport::TTxCreate: Reply",
            {"status", status},
            {"error", errorMessage},
        );
        YDB_LOG_TRACE("Message",
            {"message", response->Record.ShortDebugString()},
        );

        auto& entry = *response->Record.MutableResponse()->MutableEntry();
        entry.SetStatus(status);
        if (errorMessage) {
            AddIssue(entry, errorMessage);
        }

        AuditLogImportStart(Request->Get()->Record, response->Record, Self);

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    // Common helper to validate destination path
    bool ValidateAndAddDestinationPath(const TString& dstPath, THashSet<TString>& dstPaths, TString& explain) {
        if (dstPath) {
            if (!dstPaths.insert(NBackup::NormalizeItemPath(dstPath)).second) {
                explain = TStringBuilder() << "Duplicate destination_path: " << dstPath;
                return false;
            }

            if (!ValidateImportDstPath(dstPath, Self, explain)) {
                return false;
            }
        }
        return true;
    }

    // S3-FS-specific FillItems
    template <typename TSettings>
    bool FillItems(TImportInfo& importInfo, const TSettings& settings, TString& explain) {
        THashSet<TString> dstPaths;

        if (!importInfo.CompileExcludeRegexps(explain)) {
            return false;
        }

        importInfo.Items.reserve(settings.items().size());
        for (ui32 itemIdx : xrange(settings.items().size())) {
            const TString& dstPath = settings.items(itemIdx).destination_path();

            if (!ValidateAndAddDestinationPath(dstPath, dstPaths, explain)) {
                return false;
            }

            if (!dstPath && importInfo.GetSource().empty()) {
                // Can not take path from schema mapping
                explain = "No common source prefix and item destination path set";
                return false;
            }

            if (!importInfo.IsExcludedFromImport(dstPath)) {
                auto& item = importInfo.Items.emplace_back(dstPath);
                item.SrcPrefix = NBackup::NormalizeExportPrefix(GetItemSource(settings, itemIdx));
                item.SrcPath = NBackup::NormalizeItemPath(GetItemSourcePathDb(settings, itemIdx));
            }
        }

        if (settings.items().size() && importInfo.Items.empty()) {
            explain = TStringBuilder() << "no items to import";
            return false;
        }

        return true;
    }

}; // TTxCreate

struct TSchemeShard::TImport::TTxProgress: public TSchemeShard::TXxport::TTxBase {
    using EState = TImportInfo::EState;
    using ESubState = TImportInfo::TItem::ESubState;

    static constexpr ui32 IssuesSizeLimit = 2 * 1024;

    ui64 Id;
    TMaybe<ui32> ItemIdx;
    TEvPrivate::TEvImportSchemeReady::TPtr SchemeResult = nullptr;
    TEvPrivate::TEvImportSchemaMappingReady::TPtr SchemaMappingResult = nullptr;
    TEvPrivate::TEvImportSchemeQueryResult::TPtr SchemeQueryResult = nullptr;
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult = nullptr;
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult = nullptr;
    TEvIndexBuilder::TEvCreateResponse::TPtr CreateIndexResult = nullptr;
    TTxId CompletedTxId = InvalidTxId;
    bool DeferCancellationCompletion = false;

    explicit TTxProgress(TSelf* self, ui64 id, const TMaybe<ui32>& itemIdx)
        : TXxport::TTxBase(self)
        , Id(id)
        , ItemIdx(itemIdx)
    {
    }

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvImportSchemeReady::TPtr& ev)
        : TXxport::TTxBase(self)
        , SchemeResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvImportSchemaMappingReady::TPtr& ev)
        : TXxport::TTxBase(self)
        , Id(ev->Get()->ImportId)
        , SchemaMappingResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvImportSchemeQueryResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , SchemeQueryResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , AllocateResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , ModifyResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvIndexBuilder::TEvCreateResponse::TPtr& ev)
        : TXxport::TTxBase(self)
        , CreateIndexResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TTxId completedTxId)
        : TXxport::TTxBase(self)
        , CompletedTxId(completedTxId)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_IMPORT_PROGRESS;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TImport::TTxProgress: DoExecute");

        if (SchemeResult) {
            OnSchemeResult(txc, ctx);
        } else if (SchemaMappingResult) {
            OnSchemaMappingResult(txc, ctx);
        } else if (SchemeQueryResult) {
            OnSchemeQueryPreparation(txc, ctx);
        } else if (AllocateResult) {
            OnAllocateResult(txc, ctx);
        } else if (ModifyResult) {
            OnModifyResult(txc, ctx);
        } else if (CreateIndexResult) {
            OnCreateIndexResult(txc, ctx);
        } else if (CompletedTxId) {
            OnNotifyResult(txc, ctx);
        } else {
            Resume(txc, ctx);
        }

        return true;
    }

    void DoComplete(const TActorContext&) override {
        YDB_LOG_DEBUG("TImport::TTxProgress: DoComplete");
    }

private:
    void GetScheme(TImportInfo::TPtr importInfo, ui32 itemIdx, const TActorContext& ctx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        YDB_LOG_INFO("TImport::TTxProgress: Get scheme",
            {"info", importInfo->ToString()},
            {"item", item.ToString(itemIdx)},
        );

        item.SchemeGetter = ctx.RegisterWithSameMailbox(CreateSchemeGetter(
            Self->SelfId(), importInfo, itemIdx, item.ExportItemIV, GetDatabase(*Self)));
        Self->RunningImportSchemeGetters.emplace(item.SchemeGetter);
    }

    void GetSchemaMapping(TImportInfo::TPtr importInfo, const TActorContext& ctx) {
        YDB_LOG_INFO("TImport::TTxProgress: Download schema mapping",
            {"info", importInfo->ToString()},
        );

        importInfo->SchemaMappingGetter = ctx.RegisterWithSameMailbox(CreateSchemaMappingGetter(Self->SelfId(), importInfo));
        Self->RunningImportSchemeGetters.emplace(importInfo->SchemaMappingGetter);
    }

    bool CreateTable(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        Y_ABORT_UNLESS(item.Table);

        YDB_LOG_INFO("TImport::TTxProgress: CreateTable propose",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateTablePropose(Self, txId, importInfo, itemIdx, error);
        if (!propose) {
            return false;
        }

        item.SubState = ESubState::Proposed;
        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    void PrepareCurrentSqlOperation(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        const TActorContext& ctx)
    {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items[itemIdx];
        Y_ABORT_UNLESS(item.UsesSqlSchema());
        auto& plan = *item.TableSchemaPlan;
        Y_ABORT_UNLESS(plan.GetStatementCursor() < static_cast<ui32>(plan.StatementsSize()));
        auto* statement = plan.MutableStatements(plan.GetStatementCursor());
        plan.SetCurrentOperationAccepted(false);

        if (plan.GetOperationCursor() < static_cast<ui32>(statement->PreparedOperationsSize())) {
            item.PreparedCreationQuery = statement->GetPreparedOperations(plan.GetOperationCursor());
            Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            AllocateTxId(*importInfo, itemIdx);
            return;
        }

        item.PreparedCreationQuery.Clear();
        plan.SetOperationCursor(0);
        plan.SetPreparationAttempt(plan.GetPreparationAttempt() + 1);
        Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);
        Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);

        item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
            Self->SelfId(), importInfo->Id, itemIdx, statement->GetSql(), GetDatabase(*Self),
            statement->GetKind(), statement->GetOrdinal(), plan.GetPreparationAttempt()));
        Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
    }

    bool BeginSqlPhase(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        TTableSchemaPlan::EPhase phase,
        const TActorContext& ctx)
    {
        auto& item = importInfo->Items[itemIdx];
        auto& plan = *item.TableSchemaPlan;
        plan.SetPhase(phase);
        plan.SetOperationCursor(0);
        plan.SetCurrentOperationAccepted(false);
        item.PreparedCreationQuery.Clear();

        if (const auto next = FindSqlStatement(plan, phase, 0)) {
            plan.SetStatementCursor(*next);
            item.State = EState::CreateSchemeObject;
            Self->PersistImportItemState(db, *importInfo, itemIdx);
            Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            PrepareCurrentSqlOperation(db, importInfo, itemIdx, ctx);
            return true;
        } else {
            Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            return false;
        }
    }

    void FinishSqlPhase(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        const TActorContext& ctx)
    {
        auto& item = importInfo->Items[itemIdx];
        auto& plan = *item.TableSchemaPlan;

        switch (plan.GetPhase()) {
            case TTableSchemaPlan::PHASE_BEFORE_DATA:
                if (!item.DstPathId) {
                    UpdateItemDstPathId(db, *importInfo, itemIdx);
                }
                for (const ui32 childIdx : item.ChildItems) {
                    auto& child = importInfo->Items.at(childIdx);
                    if (child.State == EState::Done) {
                        continue;
                    }
                    UpdateItemDstPathId(db, *importInfo, childIdx);
                    child.State = EState::Transferring;
                    Self->PersistImportItemState(db, *importInfo, childIdx);
                    AllocateTxId(*importInfo, childIdx);
                }
                item.State = EState::Transferring;
                Self->PersistImportItemState(db, *importInfo, itemIdx);
                AllocateTxId(*importInfo, itemIdx);
                break;

            case TTableSchemaPlan::PHASE_AFTER_INDEXES:
                if (AppData()->FeatureFlags.GetEnableChangefeedsImport()
                    && BeginSqlPhase(db, importInfo, itemIdx, TTableSchemaPlan::PHASE_CHANGEFEEDS, ctx)) {
                    break;
                }
                item.State = EState::Done;
                Self->PersistImportItemState(db, *importInfo, itemIdx);
                break;

            case TTableSchemaPlan::PHASE_CHANGEFEEDS:
                if (TString error; !FilterSqlChangefeeds(db, *importInfo, itemIdx, error)) {
                    return CancelAndPersist(
                        db, importInfo, itemIdx, error,
                        "incomplete SQL changefeed backup");
                }
                if (item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size()) {
                    item.State = EState::CreateChangefeed;
                    item.ChangefeedState = TItem::EChangefeedState::CreateConsumers;
                    plan.SetCurrentOperationAccepted(false);
                    Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
                    Self->PersistImportItemState(db, *importInfo, itemIdx);
                    AllocateTxId(*importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                    Self->PersistImportItemState(db, *importInfo, itemIdx);
                }
                break;
        }
    }

    bool HasPendingMaterializedChildren(const TImportInfo& importInfo, const TItem& item) const {
        return AnyOf(item.ChildItems, [&](ui32 childIdx) {
            return childIdx < importInfo.Items.size()
                && importInfo.Items[childIdx].State != EState::Done;
        });
    }

    void ContinueSqlTableAfterData(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        const TActorContext& ctx)
    {
        auto& item = importInfo->Items.at(itemIdx);
        Y_ABORT_UNLESS(item.UsesSqlSchema());

        if (HasPendingMaterializedChildren(*importInfo, item)) {
            item.State = EState::WaitingForIndexData;
            Self->PersistImportItemState(db, *importInfo, itemIdx);
            return;
        }

        if (PrepareNextBuildableIndex(*importInfo, itemIdx, item)) {
            item.State = EState::BuildIndexes;
            Self->PersistImportItemState(db, *importInfo, itemIdx);
            AllocateTxId(*importInfo, itemIdx);
        } else if (!BeginSqlPhase(
                db, importInfo, itemIdx, TTableSchemaPlan::PHASE_AFTER_INDEXES, ctx)) {
            FinishSqlPhase(db, importInfo, itemIdx, ctx);
        }
    }

    void AdvanceSqlOperation(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        const TActorContext& ctx,
        bool continueExecution = true)
    {
        auto& item = importInfo->Items[itemIdx];
        auto& plan = *item.TableSchemaPlan;
        Y_ABORT_UNLESS(plan.GetStatementCursor() < static_cast<ui32>(plan.StatementsSize()));
        auto* statement = plan.MutableStatements(plan.GetStatementCursor());

        plan.SetFirstMutationCompleted(true);
        plan.SetOperationCursor(plan.GetOperationCursor() + 1);
        plan.SetCurrentOperationAccepted(false);
        item.PreparedCreationQuery.Clear();
        Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);

        if (!continueExecution) {
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            return;
        }

        if (plan.GetOperationCursor() < static_cast<ui32>(statement->PreparedOperationsSize())) {
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            PrepareCurrentSqlOperation(db, importInfo, itemIdx, ctx);
            return;
        }

        if (const auto next = FindSqlStatement(
                plan, plan.GetPhase(), plan.GetStatementCursor() + 1)) {
            plan.SetStatementCursor(*next);
            plan.SetOperationCursor(0);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            PrepareCurrentSqlOperation(db, importInfo, itemIdx, ctx);
            return;
        }

        Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
        FinishSqlPhase(db, importInfo, itemIdx, ctx);
    }

    void SelectProtobufFallback(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        TString reason)
    {
        auto& item = importInfo->Items[itemIdx];
        Y_ABORT_UNLESS(item.CanFallbackToProtobuf());
        const TString fallbackReason = reason;
        if (auto actor = std::exchange(item.SchemeQueryExecutor, {})) {
            Send(actor, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeQueryExecutors.erase(actor);
        }

        TMaybe<ui32> expectedMaterializedChildren;
        if (const auto& indexes = item.Metadata.GetIndexes()) {
            expectedMaterializedChildren = static_cast<ui32>(indexes->size());
        } else if (item.Table) {
            ui32 count = 0;
            bool allIndexTypesKnown = true;
            for (const auto& index : item.Table->indexes()) {
                const auto indexType = NTableIndex::TryConvertIndexType(
                    index.type_case(), AppData()->FeatureFlags.GetEnableCompactFulltextIndex());
                if (!indexType) {
                    allIndexTypesKnown = false;
                    break;
                }
                const TVector<TString> indexColumns(
                    index.index_columns().begin(), index.index_columns().end());
                count += static_cast<ui32>(NTableIndex::GetImplTables(*indexType, indexColumns).size());
            }
            if (allIndexTypesKnown) {
                expectedMaterializedChildren = count;
            }
        }

        const bool incompleteMaterializedBackup = expectedMaterializedChildren.Defined()
            && item.ChildItems.size() < *expectedMaterializedChildren;
        const bool incompleteChangefeedBackup = AnyOf(
            item.Changefeeds.GetChangefeeds(),
            [](const auto& changefeed) {
                return !changefeed.HasChangefeed() || !changefeed.HasTopic();
            });
        const auto populationMode = importInfo->GetIndexPopulationMode();

        item.ResetToProtobufSchema(std::move(reason));
        item.NextIndexIdx = 0;
        item.NextChangefeedIdx = 0;
        if (incompleteMaterializedBackup
            && populationMode == Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO) {
            item.TableSchemaPlan->SetForceBuildIndexes(true);
        }

        if ((incompleteMaterializedBackup
                && populationMode == Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT)
            || incompleteChangefeedBackup) {
            Self->PersistImportItemScheme(db, *importInfo, itemIdx);
            Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);
            return CancelAndPersist(
                db,
                importInfo,
                itemIdx,
                TStringBuilder() << "table SQL candidate failed: " << fallbackReason
                    << "; protobuf candidate failed: required "
                    << (incompleteChangefeedBackup ? "changefeed" : "materialized index")
                    << " backup is incomplete",
                "cannot select protobuf table fallback");
        }

        for (const ui32 childIdx : item.ChildItems) {
            auto& child = importInfo->Items.at(childIdx);
            child.State = ShouldRestoreIndexImplementationData(
                    populationMode, incompleteMaterializedBackup)
                ? EState::Waiting
                : EState::Done;
            child.WaitTxId = InvalidTxId;
            child.Issue.clear();
            Self->PersistImportItemState(db, *importInfo, childIdx);
        }

        item.State = EState::CreateSchemeObject;
        item.SubState = ESubState::AllocateTxId;
        Self->PersistImportItemScheme(db, *importInfo, itemIdx);
        Self->PersistImportItemPreparedCreationQuery(db, *importInfo, itemIdx);
        Self->PersistImportItemState(db, *importInfo, itemIdx);
        AllocateTxId(*importInfo, itemIdx);
    }

    bool NormalizeSqlIndexSettingsPhases(
        TImportInfo& importInfo,
        ui32 itemIdx,
        TString& error)
    {
        auto& item = importInfo.Items.at(itemIdx);
        auto& plan = *item.TableSchemaPlan;
        for (auto& statement : *plan.MutableStatements()) {
            if (!statement.GetPopulationDependentIndexSettings()) {
                continue;
            }

            const auto index = FindIf(plan.GetIndexes(), [&](const auto& candidate) {
                return candidate.GetName() == statement.GetIndexName();
            });
            if (index == plan.GetIndexes().end()) {
                error = TStringBuilder() << "ALTER INDEX refers to unknown SQL index "
                    << statement.GetIndexName().Quote();
                return false;
            }

            const bool buildBeforeSettings = !IsLocalIndexType(index->GetType())
                && NeedToBuildIndexes(importInfo, itemIdx);
            statement.SetPhase(buildBeforeSettings
                ? TTableSchemaPlan::PHASE_AFTER_INDEXES
                : TTableSchemaPlan::PHASE_BEFORE_DATA);
        }
        return true;
    }

    bool FilterSqlMaterializedChildren(
        NIceDb::TNiceDb& db,
        TImportInfo& importInfo,
        ui32 itemIdx,
        TString& error,
        bool& canFallback)
    {
        auto& item = importInfo.Items[itemIdx];
        Y_ABORT_UNLESS(item.TableSchemaPlan);
        canFallback = false;
        const auto populationMode = importInfo.GetIndexPopulationMode();
        if (populationMode
            == Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_BUILD) {
            for (const ui32 childIdx : item.ChildItems) {
                auto& child = importInfo.Items.at(childIdx);
                child.State = EState::Done;
                child.Issue.clear();
                Self->PersistImportItemState(db, importInfo, childIdx);
            }
            if (!NormalizeSqlIndexSettingsPhases(importInfo, itemIdx, error)) {
                canFallback = true;
                return false;
            }
            return true;
        }

        TVector<std::pair<TString, TVector<TString>>> expectedPathsByIndex;
        THashSet<TString> expectedPaths;
        for (const auto& index : item.TableSchemaPlan->GetIndexes()) {
            const auto implTableNames = SqlIndexImplTableNames(index);
            if (!implTableNames) {
                canFallback = true;
                error = TStringBuilder() << "SQL index " << index.GetName().Quote()
                    << " has unsupported type " << static_cast<ui32>(index.GetType());
                return false;
            }

            TVector<TString> indexPaths;
            indexPaths.reserve(implTableNames->size());
            for (const auto& implTableName : *implTableNames) {
                TString path = TStringBuilder() << item.DstPathName << '/'
                    << index.GetName() << '/' << implTableName;
                if (!expectedPaths.insert(path).second) {
                    canFallback = true;
                    error = TStringBuilder() << "SQL index implementation path "
                        << path.Quote() << " is not unique";
                    return false;
                }
                indexPaths.push_back(std::move(path));
            }
            expectedPathsByIndex.emplace_back(index.GetName(), std::move(indexPaths));
        }

        THashSet<TString> availablePaths;
        for (const ui32 childIdx : item.ChildItems) {
            auto& child = importInfo.Items.at(childIdx);
            if (expectedPaths.contains(child.DstPathName)) {
                if (!availablePaths.insert(child.DstPathName).second) {
                    error = TStringBuilder() << "materialized index implementation path "
                        << child.DstPathName.Quote()
                        << " occurs more than once in backup metadata";
                    return false;
                }
            } else {
                child.State = EState::Done;
                child.Issue.clear();
                Self->PersistImportItemState(db, importInfo, childIdx);
            }
        }

        bool missingSelectedData = false;
        for (const auto& [indexName, indexPaths] : expectedPathsByIndex) {
            if (AnyOf(indexPaths, [&](const TString& path) {
                    return !availablePaths.contains(path);
                })) {
                missingSelectedData = true;
                if (populationMode
                    == Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT) {
                    error = TStringBuilder() << "SQL index " << indexName.Quote()
                        << " has no complete materialized backup";
                    return false;
                }
            }
        }

        if (missingSelectedData
            && populationMode
                == Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO) {
            item.TableSchemaPlan->SetForceBuildIndexes(true);
            for (const ui32 childIdx : item.ChildItems) {
                auto& child = importInfo.Items.at(childIdx);
                child.State = EState::Done;
                child.Issue.clear();
                Self->PersistImportItemState(db, importInfo, childIdx);
            }
        }
        if (!NormalizeSqlIndexSettingsPhases(importInfo, itemIdx, error)) {
            canFallback = true;
            return false;
        }
        return true;
    }

    bool FilterSqlChangefeeds(
        NIceDb::TNiceDb& db,
        TImportInfo& importInfo,
        ui32 itemIdx,
        TString& error)
    {
        auto& item = importInfo.Items[itemIdx];
        THashSet<TString> selectedStreams;
        for (const auto& statement : item.TableSchemaPlan->GetStatements()) {
            if (statement.GetPhase() != TTableSchemaPlan::PHASE_CHANGEFEEDS) {
                continue;
            }
            for (const auto& operation : statement.GetPreparedOperations()) {
                if (operation.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateCdcStream) {
                    selectedStreams.insert(
                        operation.GetCreateCdcStream().GetStreamDescription().GetName());
                }
            }
        }

        NKikimrSchemeOp::TImportTableChangefeeds selected;
        THashSet<TString> matchedStreams;
        for (const auto& changefeed : item.Changefeeds.GetChangefeeds()) {
            if (!changefeed.HasChangefeed()) {
                continue;
            }
            const TString& name = changefeed.GetChangefeed().name();
            if (!selectedStreams.contains(name)) {
                if (matchedStreams.contains(name)) {
                    error = TStringBuilder() << "SQL changefeed " << name.Quote()
                        << " occurs more than once in backup metadata";
                    return false;
                }
                continue;
            }
            if (!changefeed.HasTopic()) {
                error = TStringBuilder() << "SQL changefeed " << name.Quote()
                    << " has no required consumer sidecar";
                return false;
            }
            *selected.AddChangefeeds() = changefeed;
            matchedStreams.insert(name);
            selectedStreams.erase(name);
        }
        if (!selectedStreams.empty()) {
            const TString& missingStream = *selectedStreams.begin();
            error = TStringBuilder() << "SQL changefeed "
                << missingStream.Quote()
                << " has no required consumer sidecar";
            return false;
        }
        item.Changefeeds = std::move(selected);
        item.NextChangefeedIdx = 0;
        Self->PersistImportItemScheme(db, importInfo, itemIdx);
        return true;
    }

    void ProcessSysViewRestore(TTransactionContext& txc, TImportInfo::TPtr importInfo, ui32 itemIdx, const TActorContext& ctx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);
        Y_ABORT_UNLESS(item.SysView);

        NIceDb::TNiceDb db(txc.DB);
        NYql::TIssues issues;

        YDB_LOG_INFO("TImport::TTxProgress: ProcessSysViewRestore",
            {"info", importInfo->ToString()},
            {"item", item.ToString(itemIdx)},
        );

        const auto ev = DescribePath(Self, ctx, item.DstPathName);
        const auto& describeResult = ev->GetRecord();
        const auto status = describeResult.GetStatus();

        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            YDB_LOG_INFO("TImport::TTxProgress: ProcessSysViewRestore: system view does not exist",
                {"item", item.ToString(itemIdx)},
            );

            item.State = EState::Done;
            return;
        } else if (status == NKikimrScheme::StatusSuccess) {
            Ydb::Table::DescribeSystemViewResult describeSysViewResult;
            Ydb::StatusIds_StatusCode status;
            TString error;

            const auto& pathDescription = describeResult.GetPathDescription();
            if (!FillSysViewDescription(describeSysViewResult, pathDescription, status, error)) {
                YDB_LOG_INFO("TImport::TTxProgress: ProcessSysViewRestore: path is not a system view",
                    {"item", item.ToString(itemIdx)},
                );

                item.State = EState::Done;
                return;
            }

            const auto compatibilityStatus = NYdb::NDump::CheckSysViewCompatibility(*item.SysView, describeSysViewResult);
            if (!compatibilityStatus.IsSuccess()) {
                YDB_LOG_ERROR("TImport::TTxProgress: ProcessSysViewRestore: system view compatibility check failed",
                    {"item", item.ToString(itemIdx)},
                );

                return CancelAndPersist(db, importInfo, itemIdx, compatibilityStatus.GetIssues().ToString(),
                    "sysview compatibility check failed");
            } else {
                if (item.Permissions.Empty()) {
                    item.State = EState::Done;
                    return;
                } else {
                    YDB_LOG_INFO("TImport::TTxProgress: ProcessSysViewRestore: needs to restore ACL",
                        {"item", item.ToString(itemIdx)},
                    );

                    AllocateTxId(*importInfo, itemIdx);
                    return;
                }
            }
        } else {
            issues.AddIssue(TStringBuilder() << "can't get path'" << item.DstPathName << "' description");
            return CancelAndPersist(db, importInfo, itemIdx, issues.ToString(), "invalid describe path status");
        }
    }

    bool ReplaceSysViewACL(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        Y_ABORT_UNLESS(item.SysView);

        YDB_LOG_INFO("TImport::TTxProgress: RestoreSysViewPermissions propose",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto path = TPath::Resolve(item.DstPathName, Self);
        Y_ABORT_UNLESS(path);

        // Only restore permissions, don't create the system view itself
        auto propose = MakeModifySchemeTransaction(Self, txId, importInfo);
        auto& record = propose->Record;

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(path.Parent().PathString());
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpModifyACL);

        auto& op = *modifyScheme.MutableModifyACL();
        op.SetName(path.Base()->Name);

        if (!FillACL(modifyScheme, item.Permissions, error)) {
            return false;
        }

        if (AppData()->AlwaysSetSystemOwner || AppData()->FeatureFlags.GetEnableIdmPermissionsManagement()) {
            op.SetNewOwner(BUILTIN_ACL_METADATA);
        }

        item.SubState = ESubState::Proposed;
        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    bool CreateTopic(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        YDB_LOG_INFO("TImport::TTxProgress: CreateTopic propose",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateTopicPropose(Self, txId, importInfo, itemIdx, error);
        if (!propose) {
            return false;
        }

        item.SubState = ESubState::Proposed;
        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    bool ExecutePreparedQuery(TTransactionContext& txc, TImportInfo::TPtr importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items[itemIdx];

        YDB_LOG_INFO("TImport::TTxProgress: ExecutePreparedQuery",
            {"info", importInfo->ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        const bool retryUnknownSqlProposal = item.UsesSqlSchema()
            && item.WaitTxId == txId
            && !item.TableSchemaPlan->GetCurrentOperationAccepted();
        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId || retryUnknownSqlProposal);

        auto propose = MakeModifySchemeTransaction(Self, txId, *importInfo);
        auto& record = propose->Record;

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme = *item.PreparedCreationQuery;
        modifyScheme.SetInternal(true);

        const auto operationType = modifyScheme.GetOperationType();
        const bool isTableCreateOperation = IsIn({
            NKikimrSchemeOp::ESchemeOpCreateTable,
            NKikimrSchemeOp::ESchemeOpCreateIndexedTable,
            NKikimrSchemeOp::ESchemeOpCreateColumnTable,
        }, operationType);
        const bool applyTablePermissions = !item.UsesSqlSchema()
            || (IsSqlBaseCreate(item) && isTableCreateOperation);
        if (applyTablePermissions) {
            FillOwner(record, item.Permissions);
        }

        record.SetOwner(ChooseAppropriateOwner(record, AppData()));

        if (TString error; applyTablePermissions && !FillACL(modifyScheme, item.Permissions, error)) {
            NIceDb::TNiceDb db(txc.DB);
            CancelAndPersist(db, importInfo, itemIdx, error, "cannot parse permissions");
            return false;
        }

        item.SubState = ESubState::Proposed;
        if (item.UsesSqlSchema() && item.WaitTxId == InvalidTxId) {
            // The durable tx identity closes the fallback fence before the
            // proposal can be accepted. A reboot either retries this exact tx
            // or subscribes to it after CurrentOperationAccepted is durable.
            item.WaitTxId = txId;
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistImportItemState(db, *importInfo, itemIdx);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
        }

        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    void DelayObjectCreation(
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        NIceDb::TNiceDb& db,
        const TString& error,
        const TActorContext& ctx)
    {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        YDB_LOG_DEBUG("TImport::TTxProgress: delay scheme object query execution",
            {"id", importInfo->Id},
            {"itemIdx", itemIdx},
        );

        item.State = EState::Waiting;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDoneOrWaiting(stateCounts)) {
            if (CountWaitingSchemeObjects(importInfo->Items) == importInfo->WaitingSchemeObjects) {
                // No progress has been made since the last scheme object creation retry.
                if (item.UsesSqlSchema() && IsSqlBaseCreate(item)
                    && item.CanFallbackToProtobuf()) {
                    return SelectProtobufFallback(
                        db,
                        importInfo,
                        itemIdx,
                        TStringBuilder() << "SQL CREATE dependency resolution made no progress: "
                            << error);
                }
                return CancelAndPersist(
                    db, importInfo, itemIdx,
                    WithTableFallbackContext(item, error),
                    "creation query failed, no progress");
            }
            RetrySchemeObjectsQueryExecution(importInfo, db, ctx);
        }
    }

    void RetrySchemeObjectsQueryExecution(TImportInfo::TPtr importInfo, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const auto database = GetDatabase(*Self);
        TVector<ui32> retriedItems;
        for (ui32 itemIdx : xrange(importInfo->Items.size())) {
            auto& item = importInfo->Items[itemIdx];
            if (item.State != EState::Waiting) {
                continue;
            }

            if (item.UsesSqlSchema()) {
                item.State = EState::CreateSchemeObject;
                Self->PersistImportItemState(db, *importInfo, itemIdx);
                if (item.PreparedCreationQuery) {
                    AllocateTxId(*importInfo, itemIdx);
                } else {
                    PrepareCurrentSqlOperation(db, importInfo, itemIdx, ctx);
                }
            } else if (IsCreatedByQuery(item) && item.PreparedCreationQuery) {
                AllocateTxId(*importInfo, itemIdx);
            } else if (IsCreatedByQuery(item)) {
                item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
                    Self->SelfId(), importInfo->Id, itemIdx, item.CreationQuery, database
                ));
                Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
            } else {
                continue;
            }

            item.State = EState::CreateSchemeObject;
            Self->PersistImportItemState(db, *importInfo, itemIdx);

            retriedItems.emplace_back(itemIdx);
        }
        if (!retriedItems.empty()) {
            importInfo->WaitingSchemeObjects = std::ssize(retriedItems);
            YDB_LOG_DEBUG("TImport::TTxProgress: retry scheme object query execution",
                {"id", importInfo->Id},
                {"retriedItems", JoinSeq(", ", retriedItems)},
            );
        }
    }

    bool TransferData(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        YDB_LOG_INFO("TImport::TTxProgress: Restore propose",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        auto propose = RestoreTableDataPropose(Self, txId, importInfo, itemIdx, error);
        if (!propose) {
            return false;
        }
        item.SubState = ESubState::Proposed;
        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    bool CancelTransferring(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                importInfo.State = EState::Cancellation;
            }

            return false;
        }

        importInfo.State = EState::Cancellation;

        YDB_LOG_INFO("TImport::TTxProgress: cancel restore's tx",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
        );

        Send(Self->SelfId(), CancelRestoreTableDataPropose(importInfo, item.WaitTxId), 0, importInfo.Id);
        return true;
    }

    bool BuildIndex(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        YDB_LOG_INFO("TImport::TTxProgress: build index",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        auto propose = BuildIndexPropose(
            Self, txId, importInfo, itemIdx, MakeIndexBuildUid(importInfo, itemIdx), error);
        if (!propose) {
            return false;
        }
        item.SubState = ESubState::Proposed;
        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    bool CancelIndexBuilding(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                importInfo.State = EState::Cancellation;
            }

            return false;
        }

        importInfo.State = EState::Cancellation;

        YDB_LOG_INFO("TImport::TTxProgress: cancel index building",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
        );

        Send(Self->SelfId(), CancelIndexBuildPropose(Self, importInfo, item.WaitTxId), 0, importInfo.Id);
        return true;
    }

    bool CreateChangefeed(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        YDB_LOG_INFO("TImport::TTxProgress: CreateChangefeed propose",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateChangefeedPropose(Self, txId, importInfo, item, error);
        if (!propose) {
            return false;
        }

        item.SubState = ESubState::Proposed;
        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    void CreateConsumers(TImportInfo& importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        item.SubState = ESubState::Proposed;

        YDB_LOG_INFO("TImport::TTxProgress: CreateConsumers propose",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
            {"txId", txId},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId
            || (item.UsesSqlSchema() && item.WaitTxId == txId
                && !item.TableSchemaPlan->GetCurrentOperationAccepted()));

        Send(Self->SelfId(), CreateConsumersPropose(Self, txId, importInfo, item));
    }

    void AdvanceChangefeedAfterCompletion(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx)
    {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);
        Y_ABORT_UNLESS(item.State == EState::CreateChangefeed);

        if (item.UsesSqlSchema()) {
            // SQL owns creation and topic settings. The legacy sidecars are used
            // only to restore consumers, so every selected stream resumes at the
            // consumer step instead of attempting another CREATE CHANGEFEED.
            item.ChangefeedState = TItem::EChangefeedState::CreateConsumers;
            item.TableSchemaPlan->SetCurrentOperationAccepted(false);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
            if (++item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size()) {
                AllocateTxId(*importInfo, itemIdx);
            } else {
                item.State = EState::Done;
            }
            return;
        }

        if (item.ChangefeedState == TItem::EChangefeedState::CreateChangefeed) {
            item.ChangefeedState = TItem::EChangefeedState::CreateConsumers;
            AllocateTxId(*importInfo, itemIdx);
        } else if (++item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size()) {
            item.ChangefeedState = TItem::EChangefeedState::CreateChangefeed;
            AllocateTxId(*importInfo, itemIdx);
        } else {
            item.State = EState::Done;
        }
    }

    void ContinueParentAfterChildCompletion(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        const TActorContext& ctx)
    {
        const auto& item = importInfo->Items.at(itemIdx);
        if (item.State != EState::Done || item.ParentIdx == Max<ui32>()
            || item.ParentIdx >= importInfo->Items.size()) {
            return;
        }

        auto& parent = importInfo->Items[item.ParentIdx];
        if (parent.State == EState::WaitingForIndexData
            && parent.UsesSqlSchema()
            && !HasPendingMaterializedChildren(*importInfo, parent)) {
            ContinueSqlTableAfterData(db, importInfo, item.ParentIdx, ctx);
        }
    }

    void AllocateTxId(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::AllocateTxId;

        YDB_LOG_INFO("TImport::TTxProgress: Allocate txId",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(), 0, importInfo.Id);
    }

    void SubscribeTx(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::Subscribed;

        YDB_LOG_INFO("TImport::TTxProgress: Wait for completion",
            {"info", importInfo.ToString()},
            {"item", item.ToString(itemIdx)},
        );

        Y_ABORT_UNLESS(item.WaitTxId != InvalidTxId);
        Send(Self->SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(ui64(item.WaitTxId)));
    }

    TTxId GetActiveRestoreTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::Transferring);
        Y_ABORT_UNLESS(item.DstPathId);

        if (!Self->PathsById.contains(item.DstPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.DstPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateRestore) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    TTxId GetActiveCreateSchemeObjectTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::CreateSchemeObject);

        const auto path = TPath::Resolve(item.DstPathName, Self);
        if (!path.IsResolved()) {
            return InvalidTxId;
        }

        if (!IsIn({
                NKikimrSchemeOp::EPathStateCreate,
                NKikimrSchemeOp::EPathStateAlter,
            }, path.Base()->PathState)) {
            return InvalidTxId;
        }

        return path.Base()->LastTxId;
    }

    TTxId GetActiveBuildIndexId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::BuildIndexes);

        const auto uid = MakeIndexBuildUid(importInfo, itemIdx);
        const auto* infoPtr = Self->IndexBuildsByUid.FindPtr(uid);
        if (!infoPtr) {
            return InvalidTxId;
        }

        return TTxId(ui64((*infoPtr)->Id));
    }

    TTxId GetActiveCreateChangefeedTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::CreateChangefeed);
        Y_ABORT_UNLESS(item.DstPathId);

        if (!Self->PathsById.contains(item.DstPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.DstPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateAlter) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    TTxId GetActiveCreateConsumerTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::CreateChangefeed);
        Y_ABORT_UNLESS(item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateConsumers);
        if (!item.StreamImplPathId) {
            return InvalidTxId;
        }

        if (!Self->PathsById.contains(item.StreamImplPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.StreamImplPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateAlter) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    void KillChildActors(TImportInfo::TItem& item) {
        if (auto schemeGetter = std::exchange(item.SchemeGetter, {})) {
            Send(schemeGetter, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeGetters.erase(schemeGetter);
        }
        if (auto schemeQueryExecutor = std::exchange(item.SchemeQueryExecutor, {})) {
            Send(schemeQueryExecutor, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeQueryExecutors.erase(schemeQueryExecutor);
        }
    }

    void FinishCancellationIfSettled(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo)
    {
        if (DeferCancellationCompletion
            || importInfo->State != EState::Cancellation
            || importInfo->HasPendingCancellationWork()) {
            return;
        }

        importInfo->State = EState::Cancelled;
        importInfo->EndTime = TAppData::TimeProvider->Now();
        Self->PersistImportState(db, *importInfo);
        Self->EraseEncryptionKey(db, *importInfo);
        SendNotificationsIfFinished(importInfo);
        AuditLogImportEnd(*importInfo.Get(), Self);
    }

    void SettleItemAfterCancellation(
        NIceDb::TNiceDb& db,
        TImportInfo::TPtr importInfo,
        ui32 itemIdx)
    {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items[itemIdx];
        item.WaitTxId = InvalidTxId;
        item.SubState = ESubState::AllocateTxId;
        item.State = EState::Cancelled;
        Self->PersistImportItemState(db, *importInfo, itemIdx);
        FinishCancellationIfSettled(db, importInfo);
    }

    void Cancel(TImportInfo& importInfo, ui32 itemIdx, TStringBuf marker) {
        const TItem* item = nullptr;
        if (itemIdx != ui32(-1)) {
            Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
            item = &importInfo.Items.at(itemIdx);
        }

        TStringBuilder itemLogStr;
        if (item) {
            itemLogStr << ", item# " << item->ToString(itemIdx);
        }
        YDB_LOG_NOTICE(TStringBuilder() << "TImport::TTxProgress: " << marker << ", cancelling",
            {"info", importInfo.ToString()},
            {"item", itemLogStr},
        );

        importInfo.State = EState::Cancelled;

        if (auto schemaMappingGetter = std::exchange(importInfo.SchemaMappingGetter, {})) {
            Send(schemaMappingGetter, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeGetters.erase(schemaMappingGetter);
        }

        for (ui32 i : xrange(importInfo.Items.size())) {
            KillChildActors(importInfo.Items[i]);
            switch (importInfo.Items.at(i).State) {
            case EState::CreateSchemeObject:
            case EState::CreateChangefeed:
                if (importInfo.Items.at(i).WaitTxId != InvalidTxId
                    || importInfo.Items.at(i).SubState == ESubState::Proposed) {
                    // Scheme operations are not cancellable. Their durable
                    // transaction identity must remain until the result is
                    // settled, so cancellation cannot become forgettable yet.
                    importInfo.State = EState::Cancellation;
                }
                break;

            case EState::Transferring:
                CancelTransferring(importInfo, i);
                break;

            case EState::BuildIndexes:
                CancelIndexBuilding(importInfo, i);
                break;

            default:
                break;
            }
        }

        if (importInfo.State == EState::Cancelled) {
            importInfo.EndTime = TAppData::TimeProvider->Now();
        }
    }

    void CancelAndPersist(NIceDb::TNiceDb& db, TImportInfo::TPtr importInfo, ui32 itemIdx, TStringBuf issue, TStringBuf marker) {
        if (itemIdx != ui32(-1)) {
            Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
            auto& item = importInfo->Items[itemIdx];

            item.Issue = issue;
            if (item.WaitTxId == InvalidTxId) {
                // CancelAndPersist is reached only after a definitive local
                // failure or response.  Do not leave a pre-send/failed
                // proposal marker that cancellation could wait for forever.
                item.SubState = ESubState::AllocateTxId;
            }
            PersistImportItemState(db, *importInfo, itemIdx);

            if (importInfo->State != EState::Waiting) {
                if (importInfo->State == EState::Cancellation
                    && item.WaitTxId == InvalidTxId) {
                    SettleItemAfterCancellation(db, importInfo, itemIdx);
                }
                return;
            }
        } else if (issue) {
            importInfo->Issue = issue;
        }

        Cancel(*importInfo, itemIdx, marker);
        PersistImportState(db, *importInfo);
        EraseEncryptionKey(db, *importInfo);

        SendNotificationsIfFinished(importInfo);
    }

    TMaybe<TString> GetIssues(const TImportInfo::TItem& item, TTxId restoreTxId) {
        if (Self->ColumnTables.contains(item.DstPathId)) {
            TColumnTableInfo::TPtr table = Self->ColumnTables.at(item.DstPathId).GetPtr();
            return GetIssues(table, restoreTxId);
        }
        if (Self->Tables.contains(item.DstPathId)) {
            TTableInfo::TPtr table = Self->Tables.at(item.DstPathId);
            return GetIssues(table, restoreTxId);
        }
        return TStringBuilder() << "destination table metadata is missing for path id "
            << item.DstPathId;
    }

    template <typename TTable>
    TMaybe<TString> GetIssues(const TTable& table, TTxId restoreTxId) {
        Y_ABORT_UNLESS(table->RestoreHistory.contains(restoreTxId));
        const auto& result = table->RestoreHistory.at(restoreTxId);

        if (result.TotalShardCount == result.SuccessShardCount) {
            return Nothing();
        }

        TStringBuilder output;
        bool first = true;

        for (const auto& [shardId, status] : result.ShardStatuses) {
            if (status.Success) {
                continue;
            }

            if (output.size() > IssuesSizeLimit) {
                output << "... <truncated>";
                break;
            }

            if (!first) {
                output << ", ";
            }
            first = false;

            output << "shard: " << shardId << ", error: " << status.Error;
        }

        return output;
    }

    TMaybe<TString> GetIssues(TIndexBuildId indexBuildId) {
        const auto* indexInfoPtr = Self->IndexBuilds.FindPtr(indexBuildId);
        Y_ABORT_UNLESS(indexInfoPtr);
        const auto& indexInfo = *indexInfoPtr->get();

        if (indexInfo.IsDone()) {
            return Nothing();
        }

        return indexInfo.GetIssue();
    }

    TString GetIssues(const NKikimrIndexBuilder::TEvCreateResponse& proto) {
        TStringBuilder output;

        for (const auto& issue : proto.GetIssues()) {
            output << issue.message() << Endl;
        }

        return output;
    }

    void Resume(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(Self->Imports.contains(Id));
        TImportInfo::TPtr importInfo = Self->Imports.at(Id);

        YDB_LOG_DEBUG("TImport::TTxProgress: Resume",
            {"id", Id},
            {"itemIdx", ItemIdx},
        );

        switch (importInfo->State) {
            case EState::DownloadExportMetadata: {
                GetSchemaMapping(importInfo, ctx);
                break;
            }
            default: {
                if (ItemIdx) {
                    Resume(importInfo, *ItemIdx, txc, ctx);
                } else {
                    // After a reboot SubState is reconstructed in memory and
                    // WaitTxId can be absent for every item.  Recover all
                    // active operations before deciding that cancellation is
                    // settled; otherwise an early inactive item could make
                    // the import forgettable while a later mutation is live.
                    DeferCancellationCompletion = importInfo->State == EState::Cancellation;
                    for (ui32 itemIdx : xrange(importInfo->Items.size())) {
                        Resume(importInfo, itemIdx, txc, ctx);
                    }
                    if (DeferCancellationCompletion) {
                        DeferCancellationCompletion = false;
                        NIceDb::TNiceDb db(txc.DB);
                        FinishCancellationIfSettled(db, importInfo);
                    }
                }
                break;
            }
        }
    }

    void Resume(TImportInfo::TPtr importInfo, ui32 itemIdx, TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        YDB_LOG_DEBUG("TImport::TTxProgress: Resume",
            {"info", importInfo->ToString()},
            {"item", item.ToString(itemIdx)},
        );

        NIceDb::TNiceDb db(txc.DB);

        switch (importInfo->State) {
            case EState::Waiting: {
                switch (item.State) {
                case EState::GetScheme:
                    if (!Self->TableProfilesLoaded) {
                        Self->WaitForTableProfiles(Id, itemIdx);
                    } else {
                        GetScheme(importInfo, itemIdx, ctx);
                    }
                    break;

                case EState::CreateSchemeObject:
                case EState::Transferring:
                case EState::BuildIndexes:
                case EState::CreateChangefeed:
                    if (item.WaitTxId == InvalidTxId) {
                        if (item.UsesSqlSchema() && item.State == EState::CreateSchemeObject) {
                            if (item.PreparedCreationQuery) {
                                AllocateTxId(*importInfo, itemIdx);
                            } else {
                                PrepareCurrentSqlOperation(db, importInfo, itemIdx, ctx);
                            }
                        } else if (!IsCreatedByQuery(item) || item.PreparedCreationQuery) {
                            AllocateTxId(*importInfo, itemIdx);
                        } else {
                            const auto database = GetDatabase(*Self);
                            item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
                                Self->SelfId(), importInfo->Id, itemIdx, item.CreationQuery, database
                            ));
                            Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
                        }
                    } else if (item.UsesSqlSchema()
                        && IsIn({EState::CreateSchemeObject, EState::CreateChangefeed}, item.State)
                        && !item.TableSchemaPlan->GetCurrentOperationAccepted()) {
                        if (item.State == EState::CreateSchemeObject) {
                            if (!item.PreparedCreationQuery) {
                                Self->TxIdToImport.erase(item.WaitTxId);
                                item.WaitTxId = InvalidTxId;
                                Self->PersistImportItemState(db, *importInfo, itemIdx);
                                return CancelAndPersist(
                                    db,
                                    importInfo,
                                    itemIdx,
                                    "SQL proposal has a durable tx id but no prepared operation",
                                    "cannot recover table SQL proposal");
                            }
                            if (!ExecutePreparedQuery(
                                    txc, importInfo, itemIdx, item.WaitTxId)) {
                                return;
                            }
                        } else {
                            Y_ABORT_UNLESS(item.State == EState::CreateChangefeed);
                            Y_ABORT_UNLESS(item.ChangefeedState
                                == TItem::EChangefeedState::CreateConsumers);
                            CreateConsumers(*importInfo, itemIdx, item.WaitTxId);
                        }
                    } else {
                        SubscribeTx(*importInfo, itemIdx);
                    }
                    break;

                case EState::WaitingForIndexData:
                    if (!HasPendingMaterializedChildren(*importInfo, item)) {
                        ContinueSqlTableAfterData(db, importInfo, itemIdx, ctx);
                    }
                    break;

                case EState::Waiting:
                    if (AllDoneOrWaiting(CountItemsByState(importInfo->Items))) {
                        RetrySchemeObjectsQueryExecution(importInfo, db, ctx);
                    }
                    break;

                default:
                    break;
                }
            }
            break;

            case EState::Cancellation: {
                TTxId txId = InvalidTxId;

                switch (item.State) {
                case EState::CreateSchemeObject:
                    if (item.WaitTxId != InvalidTxId) {
                        if (item.UsesSqlSchema()
                            && !item.TableSchemaPlan->GetCurrentOperationAccepted()) {
                            if (!item.PreparedCreationQuery) {
                                Self->TxIdToImport.erase(item.WaitTxId);
                                item.WaitTxId = InvalidTxId;
                                return CancelAndPersist(
                                    db,
                                    importInfo,
                                    itemIdx,
                                    "SQL proposal has a durable tx id but no prepared operation",
                                    "cannot settle cancelled table SQL proposal");
                            }
                            // Recover the outcome with the same durable tx id.
                            // This never allocates a competing mutation.
                            ExecutePreparedQuery(
                                txc, importInfo, itemIdx, item.WaitTxId);
                        } else {
                            SubscribeTx(*importInfo, itemIdx);
                        }
                    } else {
                        txId = GetActiveCreateSchemeObjectTxId(*importInfo, itemIdx);
                        if (txId == InvalidTxId) {
                            SettleItemAfterCancellation(db, importInfo, itemIdx);
                        }
                    }
                    break;

                case EState::CreateChangefeed:
                    if (item.WaitTxId != InvalidTxId) {
                        if (item.UsesSqlSchema()
                            && !item.TableSchemaPlan->GetCurrentOperationAccepted()) {
                            Y_ABORT_UNLESS(item.ChangefeedState
                                == TItem::EChangefeedState::CreateConsumers);
                            CreateConsumers(*importInfo, itemIdx, item.WaitTxId);
                        } else {
                            SubscribeTx(*importInfo, itemIdx);
                        }
                    } else {
                        txId = item.ChangefeedState == TItem::EChangefeedState::CreateChangefeed
                            ? GetActiveCreateChangefeedTxId(*importInfo, itemIdx)
                            : GetActiveCreateConsumerTxId(*importInfo, itemIdx);
                        if (txId == InvalidTxId) {
                            SettleItemAfterCancellation(db, importInfo, itemIdx);
                        }
                    }
                    break;

                case EState::Transferring:
                    if (!CancelTransferring(*importInfo, itemIdx)) {
                        txId = GetActiveRestoreTxId(*importInfo, itemIdx);
                        if (txId == InvalidTxId) {
                            SettleItemAfterCancellation(db, importInfo, itemIdx);
                        }
                    }
                    break;

                case EState::BuildIndexes:
                    if (!CancelIndexBuilding(*importInfo, itemIdx)) {
                        txId = GetActiveBuildIndexId(*importInfo, itemIdx);
                        if (txId == InvalidTxId) {
                            SettleItemAfterCancellation(db, importInfo, itemIdx);
                        }
                    }
                    break;

                default:
                    break;
                }

                if (txId != InvalidTxId) {
                    item.WaitTxId = txId;
                    Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
                    Self->PersistImportItemState(db, *importInfo, itemIdx);

                    switch (item.State) {
                    case EState::CreateSchemeObject:
                    case EState::CreateChangefeed:
                        SubscribeTx(*importInfo, itemIdx);
                        break;

                    case EState::Transferring:
                        CancelTransferring(*importInfo, itemIdx);
                        break;

                    case EState::BuildIndexes:
                        CancelIndexBuilding(*importInfo, itemIdx);
                        break;

                    default:
                        break;
                    }
                }
            }
            break;

        default:
            break;
        }
    }

    void OnSchemeResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(SchemeResult);

        const auto& msg = *SchemeResult->Get();

        YDB_LOG_DEBUG("TImport::TTxProgress: OnSchemeResult",
            {"id", msg.ImportId},
            {"itemIdx", msg.ItemIdx},
            {"success", msg.Success},
        );

        if (!Self->Imports.contains(msg.ImportId)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnSchemeResult received unknown id",
                {"id", msg.ImportId},
            );
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(msg.ImportId);
        if (msg.ItemIdx >= importInfo->Items.size()) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnSchemeResult received unknown item",
                {"id", msg.ImportId},
                {"item", msg.ItemIdx},
            );
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto& item = importInfo->Items.at(msg.ItemIdx);
        if (item.SchemeGetter != SchemeResult->Sender) {
            YDB_LOG_WARN("TImport::TTxProgress: ignoring stale scheme getter result",
                {"id", msg.ImportId},
                {"itemIdx", msg.ItemIdx},
                {"sender", SchemeResult->Sender},
                {"expected", item.SchemeGetter},
            );
            return;
        }
        Self->RunningImportSchemeGetters.erase(std::exchange(item.SchemeGetter, {}));

        if (!msg.Success) {
            Self->PersistImportItemScheme(db, *importInfo, msg.ItemIdx);
            return CancelAndPersist(
                db, importInfo, msg.ItemIdx,
                WithTableFallbackContext(item, msg.Error),
                "cannot get scheme");
        }

        // Persist the selected source and the complete SQL plan before starting
        // any compiler actor or allocating a mutation transaction.
        Self->PersistImportItemScheme(db, *importInfo, msg.ItemIdx);

        if (item.UsesSqlSchema()) {
            if (!BeginSqlPhase(
                    db, importInfo, msg.ItemIdx, TTableSchemaPlan::PHASE_BEFORE_DATA, ctx)) {
                return CancelAndPersist(
                    db, importInfo, msg.ItemIdx,
                    "table SQL plan has no before-data statements",
                    "invalid table SQL plan");
            }
        } else if (IsCreatedByQuery(item)) {
            // Send the creation query to KQP to prepare.
            const auto database = GetDatabase(*Self);
            const TString source = TStringBuilder() << item.SrcPath;

            NYql::TIssues issues;
            if (!RewriteCreateQuery(item.CreationQuery, database, item.DstPathName, issues)) {
                issues.AddIssue(TStringBuilder() << "path: " << source);
                return CancelAndPersist(db, importInfo, msg.ItemIdx, issues.ToString(), "invalid creation query");
            }
            Self->PersistImportItemScheme(db, *importInfo, msg.ItemIdx);

            item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
                Self->SelfId(), msg.ImportId, msg.ItemIdx, item.CreationQuery, database
            ));
            Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
        } else if (item.Table) {
            TString error;
            if (!CreateTablePropose(Self, TTxId(), *importInfo, msg.ItemIdx, error)) {
                return CancelAndPersist(
                    db, importInfo, msg.ItemIdx,
                    WithTableFallbackContext(item, std::move(error)),
                    "invalid table scheme");
            }
        }

        if (!item.UsesSqlSchema() && !IsCreatedByQuery(item)) {
            if (item.SysView) {
                ProcessSysViewRestore(txc, importInfo, msg.ItemIdx, ctx);
            } else {
                AllocateTxId(*importInfo, msg.ItemIdx);
            }
        }

        if (item.State != EState::Done) {
            item.State = EState::CreateSchemeObject;
        }

        Self->PersistImportItemState(db, *importInfo, msg.ItemIdx);

        const TString parentSrc = importInfo->GetItemSrcPrefix(msg.ItemIdx);
        const TString parentDst = item.DstPathName;
        auto materializedIndexes = std::move(item.MaterializedIndexes);
        item.ChildItems.reserve(materializedIndexes.size());
        importInfo->Items.reserve(importInfo->Items.size() + materializedIndexes.size());

        for (auto& [indexMetadata, scheme] : materializedIndexes) {
            auto src = TStringBuilder() << parentSrc << "/" << indexMetadata.ExportPrefix;
            auto dst = TStringBuilder() << parentDst << "/" << indexMetadata.ImplTablePrefix;

            auto& childItem = importInfo->Items.emplace_back(std::move(dst));
            const ui32 childIdx = importInfo->Items.size() - 1;

            importInfo->Items.at(msg.ItemIdx).ChildItems.push_back(childIdx);

            childItem.SrcPrefix = std::move(src);
            childItem.State = EState::Waiting;
            childItem.ParentIdx = msg.ItemIdx;
            childItem.Table = std::move(scheme);

            Self->PersistNewImportItem(db, *importInfo, childIdx);
            Self->PersistImportItemScheme(db, *importInfo, childIdx);
        }

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDone(stateCounts)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        } else if (AllDoneOrWaiting(stateCounts)) {
            RetrySchemeObjectsQueryExecution(importInfo, db, ctx);
        }

        Self->PersistImportState(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }
    }

    void OnSchemaMappingResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(SchemaMappingResult);

        const auto& msg = *SchemaMappingResult->Get();

        YDB_LOG_DEBUG("TImport::TTxProgress: OnSchemaMappingResult",
            {"id", msg.ImportId},
            {"success", msg.Success},
        );

        if (!Self->Imports.contains(msg.ImportId)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnSchemaMappingResult received unknown id",
                {"id", msg.ImportId},
            );
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(msg.ImportId);
        if (importInfo->SchemaMappingGetter != SchemaMappingResult->Sender) {
            YDB_LOG_WARN("TImport::TTxProgress: ignoring stale schema mapping getter result",
                {"id", msg.ImportId},
                {"sender", SchemaMappingResult->Sender},
                {"expected", importInfo->SchemaMappingGetter},
            );
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        Self->RunningImportSchemeGetters.erase(std::exchange(importInfo->SchemaMappingGetter, {}));

        if (!msg.Success) {
            return CancelAndPersist(db, importInfo, -1,
                TStringBuilder() << "cannot get schema mapping: " << msg.Error, "cannot get schema mapping");
        }

        if (!importInfo->SchemaMapping->Items.empty()) {
            if (importInfo->GetEncryptedBackup() != importInfo->SchemaMapping->Items[0].IV.Defined()) {
                return CancelAndPersist(db, importInfo, -1,
                    importInfo->GetEncryptedBackup()
                        ? "encryption is requested, but the export is not encrypted"
                        : "the export is encrypted, but no encryption settings are specified",
                    "incorrect schema mapping");
            }
        }

        const ui32 itemsSizeBefore = importInfo->Items.size();
        const TImportInfo::TFillItemsFromSchemaMappingResult fillResult = importInfo->FillItemsFromSchemaMapping(Self);
        if (!fillResult.Success) {
            return CancelAndPersist(db, importInfo, -1, fillResult.ErrorMessage, "invalid items in schema mapping");
        }

        importInfo->State = EState::Waiting;
        PersistImportState(db, *importInfo);
        PersistSchemaMappingImportFields(db, *importInfo, itemsSizeBefore);
        Resume(txc, ctx);
    }

    void OnSchemeQueryPreparation(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(SchemeQueryResult);
        const auto& message = *SchemeQueryResult.Get()->Get();
        const TString error = std::holds_alternative<TString>(message.Result) ? std::get<TString>(message.Result) : "";

        YDB_LOG_DEBUG("TImport::TTxProgress: OnSchemeQueryPreparation",
            {"id", message.ImportId},
            {"itemIdx", message.ItemIdx},
            {"status", message.Status},
            {"error", error},
        );

        auto importInfo = Self->Imports.Value(message.ImportId, nullptr);
        if (!importInfo) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnSchemeQueryPreparation received unknown import id",
                {"id", message.ImportId},
            );
            return;
        }
        if (message.ItemIdx >= importInfo->Items.size()) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnSchemeQueryPreparation item index out of range",
                {"id", message.ImportId},
                {"itemIdx", message.ItemIdx},
                {"itemsCount", importInfo->Items.size()},
            );
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto& item = importInfo->Items[message.ItemIdx];
        if (item.SchemeQueryExecutor != SchemeQueryResult->Sender) {
            YDB_LOG_WARN("TImport::TTxProgress: ignoring stale scheme preparation result",
                {"id", message.ImportId},
                {"itemIdx", message.ItemIdx},
                {"sender", SchemeQueryResult->Sender},
                {"expected", item.SchemeQueryExecutor},
            );
            return;
        }
        if (item.UsesSqlSchema()
            && (item.TableSchemaPlan->GetStatementCursor() >= static_cast<ui32>(item.TableSchemaPlan->StatementsSize())
                || message.StatementOrdinal != item.TableSchemaPlan->GetStatements(
                    item.TableSchemaPlan->GetStatementCursor()).GetOrdinal()
                || message.PreparationAttempt != item.TableSchemaPlan->GetPreparationAttempt())) {
            YDB_LOG_WARN("TImport::TTxProgress: ignoring stale SQL table preparation result",
                {"id", message.ImportId},
                {"itemIdx", message.ItemIdx},
                {"statementOrdinal", message.StatementOrdinal},
                {"preparationAttempt", message.PreparationAttempt},
            );
            return;
        }
        Self->RunningImportSchemeQueryExecutors.erase(std::exchange(item.SchemeQueryExecutor, {}));

        if (item.UsesSqlSchema() && message.Status != Ydb::StatusIds::SUCCESS) {
            if (IsSqlBaseCreate(item) && item.CanFallbackToProtobuf()
                && message.Status == Ydb::StatusIds::SCHEME_ERROR) {
                return DelayObjectCreation(importInfo, message.ItemIdx, db, error, ctx);
            }
            if (IsSqlBaseCreate(item) && item.CanFallbackToProtobuf()
                && CanFallbackAfterPreparationFailure(message.Status)) {
                return SelectProtobufFallback(
                    db,
                    importInfo,
                    message.ItemIdx,
                    TStringBuilder() << "SQL CREATE preparation failed with status "
                        << static_cast<ui32>(message.Status) << ": " << error);
            }
            return CancelAndPersist(
                db,
                importInfo,
                message.ItemIdx,
                TStringBuilder() << "SQL statement " << message.StatementOrdinal
                    << " preparation failed: " << error,
                "table SQL preparation failed");
        }

        if (!item.UsesSqlSchema() && message.Status == Ydb::StatusIds::SCHEME_ERROR) {
            // Scheme error happens when the creation query depends on other objects that are not yet imported.
            return DelayObjectCreation(importInfo, message.ItemIdx, db, error, ctx);
        }

        if (message.Status != Ydb::StatusIds::SUCCESS || !error.empty()) {
            return CancelAndPersist(db, importInfo, message.ItemIdx, error, "creation query failed");
        }

        if (item.State == EState::CreateSchemeObject && item.UsesSqlSchema()) {
            auto& plan = *item.TableSchemaPlan;
            auto* statement = plan.MutableStatements(plan.GetStatementCursor());
            auto operations = std::get<TEvPrivate::TEvImportSchemeQueryResult::TPreparedOperations>(message.Result);

            for (const auto& operation : operations) {
                TString validationError;
                if (!ValidateSqlTableOperation(operation, *statement, validationError)) {
                    if (IsSqlBaseCreate(item) && item.CanFallbackToProtobuf()) {
                        return SelectProtobufFallback(
                            db, importInfo, message.ItemIdx,
                            TStringBuilder() << "prepared SQL CREATE validation failed: "
                                << validationError);
                    }
                    return CancelAndPersist(
                        db, importInfo, message.ItemIdx, validationError,
                        "prepared table SQL operation escaped its target");
                }
            }

            if (statement->GetKind() == TTableSchemaPlan::STATEMENT_KIND_CREATE_TABLE) {
                plan.ClearIndexes();
                for (const auto& operation : operations) {
                    if (operation.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
                        for (const auto& index : operation.GetCreateIndexedTable().GetIndexDescription()) {
                            *plan.AddIndexes() = index;
                        }
                    }
                }
                TString indexSelectionError;
                bool indexSelectionAllowsFallback = false;
                if (!FilterSqlMaterializedChildren(
                        db, *importInfo, message.ItemIdx,
                        indexSelectionError, indexSelectionAllowsFallback)) {
                    if (indexSelectionAllowsFallback
                        && IsSqlBaseCreate(item)
                        && item.CanFallbackToProtobuf()) {
                        return SelectProtobufFallback(
                            db, importInfo, message.ItemIdx,
                            TStringBuilder() << "SQL CREATE index validation failed: "
                                << indexSelectionError);
                    }
                    return CancelAndPersist(
                        db, importInfo, message.ItemIdx, indexSelectionError,
                        "table SQL index selection failed");
                }

                for (auto& operation : operations) {
                    TString normalizeError;
                    if (!NormalizeSqlTableCreate(operation, *importInfo, message.ItemIdx, normalizeError)) {
                        if (IsSqlBaseCreate(item) && item.CanFallbackToProtobuf()) {
                            return SelectProtobufFallback(
                                db, importInfo, message.ItemIdx,
                                TStringBuilder() << "SQL CREATE normalization failed: "
                                    << normalizeError);
                        }
                        return CancelAndPersist(
                            db, importInfo, message.ItemIdx, normalizeError,
                            "table SQL create normalization failed");
                    }
                }
            }

            statement->ClearPreparedOperations();
            for (auto& operation : operations) {
                *statement->AddPreparedOperations() = std::move(operation);
            }
            plan.SetOperationCursor(0);
            plan.SetCurrentOperationAccepted(false);
            item.PreparedCreationQuery = statement->GetPreparedOperations(0);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, message.ItemIdx);
            Self->PersistImportItemPreparedCreationQuery(db, *importInfo, message.ItemIdx);
            AllocateTxId(*importInfo, message.ItemIdx);
        } else if (item.State == EState::CreateSchemeObject) {
            const auto& operations = std::get<TEvPrivate::TEvImportSchemeQueryResult::TPreparedOperations>(message.Result);
            Y_ABORT_UNLESS(operations.size() == 1);
            item.PreparedCreationQuery = operations.front();
            PersistImportItemPreparedCreationQuery(db, *importInfo, message.ItemIdx);
            if (item.PreparedCreationQuery->HasReplication()) {
                // In case async replication or transfer is created, it is essential to execute modify scheme
                // After all other objects, as they do not fail in case their dependencies are not imported yet
                return DelayObjectCreation(importInfo, message.ItemIdx, db, error, ctx);
            }
            AllocateTxId(*importInfo, message.ItemIdx);
        }
    }

    void OnAllocateResult(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(AllocateResult);

        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());
        const ui64 id = AllocateResult->Cookie;

        YDB_LOG_DEBUG("TImport::TTxProgress: OnAllocateResult",
            {"txId", txId},
            {"id", id},
        );

        if (!Self->Imports.contains(id)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnAllocateResult received unknown id",
                {"id", id},
            );
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        if (importInfo->State != EState::Waiting) {
            return;
        }

        TMaybe<ui32> itemIdx;
        for (ui32 i : xrange(importInfo->Items.size())) {
            const auto& item = importInfo->Items.at(i);

            if (item.SubState != ESubState::AllocateTxId) {
                continue;
            }

            switch (item.State) {
            case EState::CreateSchemeObject:
                if (item.PreparedCreationQuery) {
                    if (!ExecutePreparedQuery(txc, importInfo, i, txId)) {
                        return;
                    }
                    itemIdx = i;
                    break;
                }
                if (item.Topic) {
                    TString error;
                    if (!CreateTopic(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(db, importInfo, i, error, "creation topic failed");
                        return;
                    }
                    itemIdx = i;
                    break;
                }
                if (item.SysView) {
                    TString error;
                    if (!ReplaceSysViewACL(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(db, importInfo, i, error, "restore sysview permissions failed");
                        return;
                    }
                    itemIdx = i;
                    break;
                }
                if (IsCreatedByQuery(item)) {
                    // We only need a txId for modify scheme transactions.
                    // If an object’s CreationQuery has not been prepared yet, it does not need a txId at this point.
                    break;
                }
                if (!Self->TableProfilesLoaded) {
                    Self->WaitForTableProfiles(id, i);
                } else if (item.Table) {
                    TString error;
                    if (!CreateTable(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(
                            db, importInfo, i,
                            WithTableFallbackContext(item, std::move(error)),
                            "invalid table scheme");
                        return;
                    }
                    itemIdx = i;
                }
                break;

            case EState::Transferring:
                if (TString error; !TransferData(*importInfo, i, txId, error)) {
                    NIceDb::TNiceDb db(txc.DB);
                    CancelAndPersist(db, importInfo, i, error, "invalid table data layout");
                    return;
                }
                itemIdx = i;
                break;

            case EState::BuildIndexes:
                if (TString error; !BuildIndex(*importInfo, i, txId, error)) {
                    NIceDb::TNiceDb db(txc.DB);
                    CancelAndPersist(db, importInfo, i, error, "invalid SQL index build plan");
                    return;
                }
                itemIdx = i;
                break;

            case EState::CreateChangefeed:
                if (item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateChangefeed) {
                    TString error;
                    if (!CreateChangefeed(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(db, importInfo, i, error, "creation changefeed failed");
                        return;
                    }
                } else {
                    if (item.UsesSqlSchema()) {
                        item.WaitTxId = txId;
                        item.TableSchemaPlan->SetCurrentOperationAccepted(false);
                        NIceDb::TNiceDb db(txc.DB);
                        Self->PersistImportItemState(db, *importInfo, i);
                        Self->PersistImportItemTableSchemaPlan(db, *importInfo, i);
                    }
                    CreateConsumers(*importInfo, i, txId);
                }
                itemIdx = i;
                break;

            default:
                break;
            }

            if (itemIdx) {
                break;
            }
        }

        if (!itemIdx) {
            return;
        }

        Y_ABORT_UNLESS(!Self->TxIdToImport.contains(txId));
        Self->TxIdToImport[txId] = {importInfo->Id, *itemIdx};
    }

    void OnModifyResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ModifyResult);
        const auto& record = ModifyResult->Get()->Record;

        YDB_LOG_DEBUG("TImport::TTxProgress: OnModifyResult",
            {"txId", record.GetTxId()},
            {"status", record.GetStatus()},
        );
        YDB_LOG_TRACE("Message",
            {"message", record.ShortDebugString()},
        );

        auto txId = TTxId(record.GetTxId());
        if (!Self->TxIdToImport.contains(txId)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnModifyResult received unknown txId",
                {"txId", txId},
            );
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToImport.at(txId);
        if (!Self->Imports.contains(id)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnModifyResult received unknown id",
                {"id", id},
            );
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);
        const bool isSqlSchemaOperation = item.UsesSqlSchema()
            && item.State == EState::CreateSchemeObject;
        const bool isSqlTrackedSchemeOperation = item.UsesSqlSchema()
            && IsIn({EState::CreateSchemeObject, EState::CreateChangefeed}, item.State);
        const bool isOwnedSchemeTransaction = IsIn({
                EState::CreateSchemeObject,
                EState::CreateChangefeed,
            }, item.State)
            && IsIn({
                NKikimrScheme::StatusMultipleModifications,
                NKikimrScheme::StatusAlreadyExists,
            }, record.GetStatus())
            && record.GetPathCreateTxId() == ui64(txId);
        const bool isOwnedSqlTransaction = isSqlTrackedSchemeOperation
            && isOwnedSchemeTransaction;
        const TTxId completedTxId = txId;

        if (importInfo->State == EState::Cancellation) {
            if (record.GetStatus() == NKikimrScheme::StatusSuccess) {
                Self->TxIdToImport.erase(txId);
                item.WaitTxId = InvalidTxId;
                if (isSqlSchemaOperation) {
                    AdvanceSqlOperation(db, importInfo, itemIdx, ctx, false);
                }
                SettleItemAfterCancellation(db, importInfo, itemIdx);
                return;
            }

            if (IsIn({EState::CreateSchemeObject, EState::CreateChangefeed}, item.State)
                && record.GetStatus() != NKikimrScheme::StatusAccepted
                && !isOwnedSchemeTransaction) {
                Self->TxIdToImport.erase(txId);
                item.WaitTxId = InvalidTxId;
                SettleItemAfterCancellation(db, importInfo, itemIdx);
                return;
            }
        }

        if (IsCreatedByQuery(item)) {
            // As for created by query objects query must be compiled to execute
            Y_ABORT_UNLESS(item.PreparedCreationQuery);

            if (ShouldDelayQueryExecutionOnError(*item.PreparedCreationQuery, record.GetStatus())) {
                return DelayObjectCreation(importInfo, itemIdx, db, record.GetReason(), ctx);
            }
        }

        if (record.GetStatus() == NKikimrScheme::StatusSuccess) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;
            item.WaitTxId = InvalidTxId;
            if (isSqlSchemaOperation) {
                AdvanceSqlOperation(
                    db, importInfo, itemIdx, ctx,
                    importInfo->State == EState::Waiting);
            } else if (importInfo->State == EState::Waiting
                && item.UsesSqlSchema()
                && item.State == EState::Transferring) {
                if (const auto issue = GetIssues(item, completedTxId)) {
                    return CancelAndPersist(
                        db, importInfo, itemIdx, *issue,
                        "issues during synchronous SQL table restore");
                }
                ContinueSqlTableAfterData(db, importInfo, itemIdx, ctx);
            } else if (importInfo->State == EState::Waiting
                && item.UsesSqlSchema()
                && item.State == EState::CreateChangefeed) {
                AdvanceChangefeedAfterCompletion(db, importInfo, itemIdx);
            } else {
                item.State = EState::Done;
            }
        } else if (record.GetStatus() != NKikimrScheme::StatusAccepted
            && !isOwnedSqlTransaction) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;
            item.WaitTxId = InvalidTxId;

            if (isSqlSchemaOperation) {
                if (importInfo->State != EState::Waiting) {
                    Self->PersistImportItemState(db, *importInfo, itemIdx);
                    return;
                }
                if (IsSqlBaseCreate(item)
                    && item.CanFallbackToProtobuf()
                    && ShouldRetrySqlBaseProposal(record.GetStatus())) {
                    return DelayObjectCreation(
                        importInfo, itemIdx, db, record.GetReason(), ctx);
                }
                if (IsSqlBaseCreate(item)
                    && item.CanFallbackToProtobuf()
                    && CanFallbackAfterProposalFailure(record.GetStatus())) {
                    SelectProtobufFallback(
                        db,
                        importInfo,
                        itemIdx,
                        TStringBuilder() << "SQL CREATE was rejected before apply: "
                            << record.GetReason());
                    Self->PersistImportState(db, *importInfo);
                    return;
                }
                return CancelAndPersist(
                    db,
                    importInfo,
                    itemIdx,
                    TStringBuilder() << "SQL statement "
                        << item.TableSchemaPlan->GetStatements(
                            item.TableSchemaPlan->GetStatementCursor()).GetOrdinal()
                        << " proposal failed: " << record.GetReason(),
                    "table SQL proposal failed");
            }

            if (IsIn({ NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists },
                record.GetStatus()
            )) {
                if (record.GetPathCreateTxId()) {
                    txId = TTxId(record.GetPathCreateTxId());
                } else if (item.State == EState::CreateSchemeObject) {
                    // In case dependency object is being created
                    return DelayObjectCreation(importInfo, itemIdx, db, record.GetReason(), ctx);
                } else if (item.State == EState::Transferring) {
                    txId = GetActiveRestoreTxId(*importInfo, itemIdx);
                } else if (item.State == EState::CreateChangefeed) {
                    if (item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateChangefeed) {
                        txId = GetActiveCreateChangefeedTxId(*importInfo, itemIdx);
                    } else {
                        txId = GetActiveCreateConsumerTxId(*importInfo, itemIdx);
                    }
                }
            }

            if (txId == InvalidTxId) {
                if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists && item.State == EState::CreateChangefeed) {
                    AdvanceChangefeedAfterCompletion(db, importInfo, itemIdx);
                } else {
                    TString error = record.GetReason();
                    if (item.State == EState::CreateSchemeObject) {
                        error = WithTableFallbackContext(item, std::move(error));
                    }
                    return CancelAndPersist(db, importInfo, itemIdx, error, "unhappy propose");
                }
            } else {
                Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
            }
        }

        if (isSqlTrackedSchemeOperation
            && (record.GetStatus() == NKikimrScheme::StatusAccepted || isOwnedSqlTransaction)) {
            item.TableSchemaPlan->SetCurrentOperationAccepted(true);
            Self->PersistImportItemTableSchemaPlan(db, *importInfo, itemIdx);
        }

        item.WaitTxId = txId;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        if (importInfo->State != EState::Waiting && item.State == EState::Transferring) {
            CancelTransferring(*importInfo, itemIdx);
            return;
        }

        if (!item.UsesSqlSchema()
            && (item.State == EState::Done || item.State == EState::CreateSchemeObject)) {
            UpdateItemDstPathId(db, *importInfo, itemIdx);
            for (auto childIdx : item.ChildItems) {
                if (importInfo->Items.at(childIdx).State != EState::Done) {
                    UpdateItemDstPathId(db, *importInfo, childIdx);
                }
            }
        }

        if (txId != InvalidTxId) {
            SubscribeTx(*importInfo, itemIdx);
        }

        ContinueParentAfterChildCompletion(db, importInfo, itemIdx, ctx);

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDone(stateCounts)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        } else if (AllDoneOrWaiting(stateCounts)) {
            RetrySchemeObjectsQueryExecution(importInfo, db, ctx);
        }

        Self->PersistImportState(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }
    }

    void UpdateItemDstPathId(NIceDb::TNiceDb& db, TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        auto path = TPath::Resolve(item.DstPathName, Self);
        Y_ABORT_UNLESS(path);

        item.DstPathId = path.Base()->PathId;
        Self->PersistImportItemDstPathId(db, importInfo, itemIdx);
    }

    void OnCreateIndexResult(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(CreateIndexResult);
        const auto& record = CreateIndexResult->Get()->Record;

        YDB_LOG_DEBUG("TImport::TTxProgress: OnCreateIndexResult",
            {"txId", record.GetTxId()},
            {"status", record.GetStatus()},
        );
        YDB_LOG_TRACE("Message",
            {"message", record.ShortDebugString()},
        );

        auto txId = TTxId(record.GetTxId());
        if (!Self->TxIdToImport.contains(txId)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnCreateIndexResult received unknown txId",
                {"txId", txId},
            );
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToImport.at(txId);
        if (!Self->Imports.contains(id)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnCreateIndexResult received unknown id",
                {"id", id},
            );
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;

            if (record.GetStatus() == Ydb::StatusIds::ALREADY_EXISTS) {
                if (item.State == EState::BuildIndexes) {
                    txId = GetActiveBuildIndexId(*importInfo, itemIdx);
                }
            }

            if (txId == InvalidTxId) {
                const TString issue = GetIssues(record);

                if (importInfo->State != EState::Waiting) {
                    if (importInfo->State == EState::Cancellation) {
                        item.WaitTxId = InvalidTxId;
                        item.Issue = issue;
                        SettleItemAfterCancellation(db, importInfo, itemIdx);
                    }
                    return;
                }

                return CancelAndPersist(
                    db, importInfo, itemIdx, issue, "unhappy index build proposal");
            }

            Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
        }

        item.WaitTxId = txId;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        if (importInfo->State != EState::Waiting) {
            CancelIndexBuilding(*importInfo, itemIdx);
            return;
        }

        SubscribeTx(*importInfo, itemIdx);
    }

    void OnNotifyResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(CompletedTxId);
        YDB_LOG_DEBUG("TImport::TTxProgress: OnNotifyResult",
            {"txId", CompletedTxId},
        );

        const auto txId = CompletedTxId;
        if (!Self->TxIdToImport.contains(txId)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnNotifyResult received unknown txId",
                {"txId", txId},
            );
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToImport.at(txId);
        if (!Self->Imports.contains(id)) {
            YDB_LOG_ERROR("TImport::TTxProgress: OnNotifyResult received unknown id",
                {"id", id},
            );
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        if (importInfo->State != EState::Waiting) {
            if (importInfo->State == EState::Cancellation) {
                item.WaitTxId = InvalidTxId;
                Self->TxIdToImport.erase(txId);
                if (item.UsesSqlSchema() && item.State == EState::CreateSchemeObject) {
                    // The transaction completed while cancellation was in
                    // progress. Record the mutation fence/cursor, but never
                    // schedule the next statement.
                    AdvanceSqlOperation(db, importInfo, itemIdx, ctx, false);
                }
                SettleItemAfterCancellation(db, importInfo, itemIdx);
            } else if (item.UsesSqlSchema() && item.State == EState::CreateSchemeObject) {
                item.WaitTxId = InvalidTxId;
                Self->TxIdToImport.erase(txId);
                AdvanceSqlOperation(db, importInfo, itemIdx, ctx, false);
                Self->PersistImportItemState(db, *importInfo, itemIdx);
            }
            return;
        }

        item.WaitTxId = InvalidTxId;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        Self->TxIdToImport.erase(txId);

        switch (item.State) {
        case EState::CreateSchemeObject:
            if (item.UsesSqlSchema()) {
                AdvanceSqlOperation(db, importInfo, itemIdx, ctx, true);
                break;
            } else if (IsCreatedByQuery(item)) {
                item.State = EState::Done;
                break;
            } else if (item.Topic) {
                item.State = EState::Done;
                break;
            }
            if (item.Table) {
                for (auto childIdx : item.ChildItems) {
                    Y_ABORT_UNLESS(childIdx < importInfo->Items.size());
                    auto& childItem = importInfo->Items.at(childIdx);

                    if (childItem.State == EState::Done) {
                        continue;
                    }

                    childItem.State = EState::Transferring;
                    Self->PersistImportItemState(db, *importInfo, childIdx);
                    AllocateTxId(*importInfo, childIdx);
                }
            } else {
                Y_ABORT("Create Scheme Object: schema objects are empty");
            }
            item.State = EState::Transferring;
            AllocateTxId(*importInfo, itemIdx);
            break;

        case EState::Transferring:
            if (const auto issue = GetIssues(item, txId)) {
                item.Issue = *issue;
                Cancel(*importInfo, itemIdx, "issues during restore " + *issue);
                Self->EraseEncryptionKey(db, *importInfo);
            } else {
                if (item.UsesSqlSchema()) {
                    ContinueSqlTableAfterData(db, importInfo, itemIdx, ctx);
                } else if (PrepareNextBuildableIndex(*importInfo, itemIdx, item)) {
                    item.State = EState::BuildIndexes;
                    AllocateTxId(*importInfo, itemIdx);
                } else if (item.NextChangefeedIdx < item.Changefeeds.changefeeds_size() &&
                           AppData()->FeatureFlags.GetEnableChangefeedsImport()) {
                    item.State = EState::CreateChangefeed;
                    AllocateTxId(*importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                }
            }
            break;

        case EState::BuildIndexes:
            if (const auto issue = GetIssues(TIndexBuildId(ui64(txId)))) {
                item.Issue = *issue;
                Cancel(*importInfo, itemIdx, "issues during index building");
                Self->EraseEncryptionKey(db, *importInfo);
            } else {
                if (item.Table) {
                    ++item.NextIndexIdx;
                }
                if (PrepareNextBuildableIndex(*importInfo, itemIdx, item)) {
                    AllocateTxId(*importInfo, itemIdx);
                } else if (item.UsesSqlSchema()) {
                    if (!BeginSqlPhase(
                            db, importInfo, itemIdx, TTableSchemaPlan::PHASE_AFTER_INDEXES, ctx)) {
                        FinishSqlPhase(db, importInfo, itemIdx, ctx);
                    }
                } else if (item.NextChangefeedIdx < item.Changefeeds.changefeeds_size() &&
                           AppData()->FeatureFlags.GetEnableChangefeedsImport()) {
                    item.State = EState::CreateChangefeed;
                    AllocateTxId(*importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                }
            }
            break;

        case EState::CreateChangefeed:
            AdvanceChangefeedAfterCompletion(db, importInfo, itemIdx);
            break;

        default:
            return SendNotificationsIfFinished(importInfo);
        }

        ContinueParentAfterChildCompletion(db, importInfo, itemIdx, ctx);

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDone(stateCounts)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        } else if (AllDoneOrWaiting(stateCounts)) {
            RetrySchemeObjectsQueryExecution(importInfo, db, ctx);
        }

        Self->PersistImportItemState(db, *importInfo, itemIdx);
        Self->PersistImportState(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }
    }

}; // TTxProgress

ITransaction* TSchemeShard::CreateTxCreateImport(TEvImport::TEvCreateImportRequest::TPtr& ev) {
    return new TImport::TTxCreate(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(ui64 id, const TMaybe<ui32>& itemIdx) {
    return new TImport::TTxProgress(this, id, itemIdx);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvPrivate::TEvImportSchemeReady::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvPrivate::TEvImportSchemaMappingReady::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvPrivate::TEvImportSchemeQueryResult::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvIndexBuilder::TEvCreateResponse::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TTxId completedTxId) {
    return new TImport::TTxProgress(this, completedTxId);
}

} // NSchemeShard
} // NKikimr

#undef YDB_LOG_THIS_FILE_COMPONENT

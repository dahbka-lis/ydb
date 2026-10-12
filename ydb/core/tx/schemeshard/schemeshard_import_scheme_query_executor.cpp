#include "schemeshard_import_scheme_query_executor.h"

#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/time_provider/time_provider.h>

#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IMPORT

using namespace NKikimr::NKqp;

namespace NKikimr::NSchemeShard {

class TSchemeQueryExecutor: public TActorBootstrapped<TSchemeQueryExecutor> {
    using TStatementKind = NKikimrImport::TTableSchemaImportPlan::EStatementKind;
    using TPreparedOperations = TEvPrivate::TEvImportSchemeQueryResult::TPreparedOperations;

    const NKikimrSchemeOp::TModifyScheme* ExtractExpectedOperation(
        const NKqpProto::TKqpSchemeOperation& operation) const
    {
        using TSchemeOperation = NKqpProto::TKqpSchemeOperation;

        switch (ExpectedKind) {
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_CREATE_TABLE:
                switch (operation.GetOperationCase()) {
                    case TSchemeOperation::kCreateTable:
                        return &operation.GetCreateTable();
                    case TSchemeOperation::kCreateColumnTable:
                        return &operation.GetCreateColumnTable();
                    default:
                        return nullptr;
                }
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_SEQUENCE:
                return operation.GetOperationCase() == TSchemeOperation::kAlterSequence
                    ? &operation.GetAlterSequence()
                    : nullptr;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_TABLE:
                switch (operation.GetOperationCase()) {
                    case TSchemeOperation::kAlterTable:
                        return &operation.GetAlterTable();
                    case TSchemeOperation::kAlterColumnTable:
                        return &operation.GetAlterColumnTable();
                    default:
                        return nullptr;
                }
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_OBJECT:
                return operation.GetObjectType() == "TABLE"
                    && operation.GetOperationCase() == TSchemeOperation::kAlterColumnTable
                    ? &operation.GetAlterColumnTable()
                    : nullptr;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_TOPIC:
                return operation.GetOperationCase() == TSchemeOperation::kAlterTopic
                    ? &operation.GetAlterTopic()
                    : nullptr;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_UNSPECIFIED:
                switch (operation.GetOperationCase()) {
                    case TSchemeOperation::kCreateView:
                        return &operation.GetCreateView();
                    case TSchemeOperation::kCreateReplication:
                        return &operation.GetCreateReplication();
                    case TSchemeOperation::kCreateTransfer:
                        return &operation.GetCreateTransfer();
                    case TSchemeOperation::kCreateExternalDataSource:
                        return &operation.GetCreateExternalDataSource();
                    case TSchemeOperation::kCreateExternalTable:
                        return &operation.GetCreateExternalTable();
                    default:
                        return nullptr;
                }
        }
        return nullptr;
    }

    bool IsExpectedModifyOperation(const NKikimrSchemeOp::TModifyScheme& operation) const {
        using NKikimrSchemeOp::EOperationType;

        switch (ExpectedKind) {
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_CREATE_TABLE:
                return operation.GetOperationType() == EOperationType::ESchemeOpCreateTable
                    || operation.GetOperationType() == EOperationType::ESchemeOpCreateIndexedTable
                    || operation.GetOperationType() == EOperationType::ESchemeOpCreateColumnTable;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_SEQUENCE:
                return operation.GetOperationType() == EOperationType::ESchemeOpAlterSequence;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_TABLE:
                return operation.GetOperationType() == EOperationType::ESchemeOpAlterTable
                    || operation.GetOperationType() == EOperationType::ESchemeOpAlterColumnTable
                    || operation.GetOperationType() == EOperationType::ESchemeOpCreateTableIndex
                    || operation.GetOperationType() == EOperationType::ESchemeOpAlterTableIndex
                    || operation.GetOperationType() == EOperationType::ESchemeOpCreateCdcStream;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_OBJECT:
                return operation.GetOperationType() == EOperationType::ESchemeOpAlterColumnTable
                    || operation.GetOperationType() == EOperationType::ESchemeOpAlterTable;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_TOPIC:
                return operation.GetOperationType() == EOperationType::ESchemeOpAlterPersQueueGroup;
            case NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_UNSPECIFIED:
                return true;
        }
        return false;
    }

    std::unique_ptr<TEvKqp::TEvCompileRequest> BuildCompileRequest() {
        UserToken.Reset(MakeIntrusive<NACLib::TUserToken>(""));

        TKqpQuerySettings querySettings(NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_QUERY);
        querySettings.IsInternalCall = true;

        GUCSettings = std::make_shared<TGUCSettings>();

        TKqpQueryId query(
            TString(DefaultKikimrPublicClusterName), // cluster
            Database, // database
            "", // database id
            UserToken->GetUserSID(), // user sid
            SchemeQuery, // query text
            querySettings, // query settings
            nullptr, // query parameter types
            *GUCSettings // GUC settings
        );

        // TO DO: get default query timeout from the app config
        auto deadline = TAppData::TimeProvider->Now() + TDuration::Minutes(1);
        TKqpCounters kqpCounters(AppData()->Counters, &TlsActivationContext->AsActorContext());
        IsInterestedInResult = std::make_shared<std::atomic<bool>>(true);
        UserRequestContext.Reset(MakeIntrusive<TUserRequestContext>());

        return std::make_unique<TEvKqp::TEvCompileRequest>(
            UserToken, // user token
            "", // client address
            Nothing(), // query uid in query cache
            query, // TKqpQueryId
            false, // keep in query cache
            true, // is query action prepare?
            false, // per statement result
            deadline, // deadline
            kqpCounters.GetDbCounters(Database), // db counters
            GUCSettings, // GUC settings
            Nothing(), // application name
            IsInterestedInResult, // is still interested in result?
            UserRequestContext // user request context
        );
    }

    void PrepareSchemeQuery() {
        if (!Send(MakeKqpCompileServiceID(SelfId().NodeId()), BuildCompileRequest().release())) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "cannot send query request");
        }
        Become(&TThis::StateExecute);
    }

    void HandleCompileResponse(const TEvKqp::TEvCompileResponse::TPtr& ev) {
        const auto* result = ev->Get()->CompileResult.get();
        if (!result) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "empty compile response");
        }

        YDB_LOG_DEBUG("TSchemeQueryExecutor HandleCompileResponse",
            {"self", SelfId()},
            {"status", result->Status},
        );

        if (result->Status != Ydb::StatusIds::SUCCESS) {
            return Finish(result->Status, result->Issues.ToOneLineString());
        }
        if (!result->PreparedQuery) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "no prepared query");
        }
        const auto& transactions = result->PreparedQuery->GetPhysicalQuery().GetTransactions();
        if (transactions.empty()) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "empty transactions");
        }
        TPreparedOperations operations;
        operations.reserve(transactions.size());
        for (ui32 index = 0; index < transactions.size(); ++index) {
            const auto& transaction = transactions[index];
            if (!transaction.HasSchemeOperation()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder()
                    << "physical transaction " << index << " has no scheme operation");
            }
            const auto& schemeOperation = transaction.GetSchemeOperation();
            const auto* operation = ExtractExpectedOperation(schemeOperation);
            if (!operation || !IsExpectedModifyOperation(*operation)) {
                return Finish(Ydb::StatusIds::UNSUPPORTED, TStringBuilder()
                    << "unsupported physical scheme operation case "
                    << static_cast<ui32>(schemeOperation.GetOperationCase())
                    << " in transaction " << index);
            }
            operations.push_back(*operation);
        }
        if (ExpectedKind == NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_UNSPECIFIED
            && operations.size() != 1) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR,
                "legacy creation query produced multiple physical operations");
        }

        return Finish(result->Status, std::move(operations));
    }

    void Finish(Ydb::StatusIds::StatusCode status, std::variant<TString, TPreparedOperations> result) {
        YDB_LOG_INFO("TSchemeQueryExecutor Reply",
            {"self", SelfId()},
            {"status", status},
        );

        std::visit([&]<typename T>(T& value) {
            if constexpr (std::is_same_v<T, TString>) {
                YDB_LOG_DEBUG("TSchemeQueryExecutor Reply",
                    {"self", SelfId()},
                    {"status", status},
                    {"error", value},
                );
            } else if constexpr (std::is_same_v<T, TPreparedOperations>) {
                YDB_LOG_DEBUG("TSchemeQueryExecutor Reply",
                    {"self", SelfId()},
                    {"status", status},
                    {"preparedOperations", value.size()},
                );
            }
            Send(ReplyTo, new TEvPrivate::TEvImportSchemeQueryResult(
                ImportId, ItemIdx, StatementOrdinal, PreparationAttempt, status, std::move(value)));
        }, result);

        PassAway();
    }

public:

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMPORT_SCHEME_QUERY_EXECUTOR;
    }

    TSchemeQueryExecutor(
        TActorId replyTo,
        ui64 importId,
        ui32 itemIdx,
        const TString& schemeQuery,
        const TString& database,
        TStatementKind expectedKind,
        ui32 statementOrdinal,
        ui64 preparationAttempt
    )
        : ReplyTo(replyTo)
        , ImportId(importId)
        , ItemIdx(itemIdx)
        , SchemeQuery(schemeQuery)
        , Database(database)
        , ExpectedKind(expectedKind)
        , StatementOrdinal(statementOrdinal)
        , PreparationAttempt(preparationAttempt)
    {
    }

    void Bootstrap() {
        PrepareSchemeQuery();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateExecute) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvCompileResponse, HandleCompileResponse);
        default:
            return StateBase(ev);
        }
    }

private:

    TActorId ReplyTo;
    ui64 ImportId;
    ui32 ItemIdx;
    TString SchemeQuery;
    TString Database;
    TStatementKind ExpectedKind;
    ui32 StatementOrdinal;
    ui64 PreparationAttempt;

    // The following pointer-type event arguments are necessary for constructing the compile request.
    // These pointers must remain valid until the compilation response is received.
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TGUCSettings::TPtr GUCSettings;
    std::shared_ptr<std::atomic<bool>> IsInterestedInResult;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;

}; // TSchemeQueryExecutor

IActor* CreateSchemeQueryExecutor(NActors::TActorId replyTo, ui64 importId, ui32 itemIdx,
        const TString& schemeQuery, const TString& database,
        NKikimrImport::TTableSchemaImportPlan::EStatementKind expectedKind,
        ui32 statementOrdinal, ui64 preparationAttempt) {
    return new TSchemeQueryExecutor(
        replyTo, importId, itemIdx, schemeQuery, database, expectedKind, statementOrdinal, preparationAttempt);
}

} // NKikimr::NSchemeShard

#undef YDB_LOG_THIS_FILE_COMPONENT

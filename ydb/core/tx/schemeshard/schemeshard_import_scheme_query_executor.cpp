#include "schemeshard_import_scheme_query_executor.h"

#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/time_provider/time_provider.h>

using namespace NKikimr::NKqp;

namespace NKikimr::NSchemeShard {

TMaybe<TString> GetPreparedQueryTargetPath(
    const NKikimrSchemeOp::TModifyScheme& modifyScheme)
{
    TString name;
    switch (modifyScheme.GetOperationType()) {
    case NKikimrSchemeOp::ESchemeOpCreateTable:
        if (!modifyScheme.HasCreateTable()) {
            return Nothing();
        }
        name = modifyScheme.GetCreateTable().GetName();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
        if (!modifyScheme.HasCreateIndexedTable()
            || !modifyScheme.GetCreateIndexedTable().HasTableDescription())
        {
            return Nothing();
        }
        name = modifyScheme.GetCreateIndexedTable().GetTableDescription().GetName();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateView:
        if (!modifyScheme.HasCreateView()) {
            return Nothing();
        }
        name = modifyScheme.GetCreateView().GetName();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateReplication:
    case NKikimrSchemeOp::ESchemeOpCreateTransfer:
        if (!modifyScheme.HasReplication()) {
            return Nothing();
        }
        name = modifyScheme.GetReplication().GetName();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource:
        if (!modifyScheme.HasCreateExternalDataSource()) {
            return Nothing();
        }
        name = modifyScheme.GetCreateExternalDataSource().GetName();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateExternalTable:
        if (!modifyScheme.HasCreateExternalTable()) {
            return Nothing();
        }
        name = modifyScheme.GetCreateExternalTable().GetName();
        break;
    default:
        return Nothing();
    }

    return CanonizePath(JoinPath({modifyScheme.GetWorkingDir(), name}));
}

class TSchemeQueryExecutor: public TActorBootstrapped<TSchemeQueryExecutor> {

    void FinishPrepared(
        Ydb::StatusIds::StatusCode status,
        NKikimrSchemeOp::TModifyScheme preparedQuery)
    {
        const auto targetPath = GetPreparedQueryTargetPath(preparedQuery);
        const TString canonicalDestination = CanonizePath(DestinationPath);
        if (!targetPath || *targetPath != canonicalDestination) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder()
                << "Prepared creation query target path "
                << (targetPath ? targetPath->Quote() : "<unknown>")
                << " does not match destination path " << canonicalDestination.Quote());
        }

        Finish(status, std::move(preparedQuery));
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

        LOG_D("TSchemeQueryExecutor HandleCompileResponse"
            << ", self: " << SelfId()
            << ", status: " << result->Status;
        );

        if (result->Status != Ydb::StatusIds::SUCCESS) {
            return Finish(result->Status, result->Issues.ToOneLineString());
        }
        if (!result->PreparedQuery) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "no prepared query");
        }
        const auto& transactions = result->PreparedQuery->GetPhysicalQuery().GetTransactions();
        if (transactions.size() != 1) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected exactly one physical transaction");
        }
        if (transactions[0].GetType() != NKqpProto::TKqpPhyTx::TYPE_SCHEME
            || !transactions[0].HasSchemeOperation())
        {
            return Finish(Ydb::StatusIds::GENERIC_ERROR,
                "expected a physical scheme transaction");
        }

        const auto& operation = transactions[0].GetSchemeOperation();
        switch (CreationQueryPathType) {
        case NKikimrSchemeOp::EPathTypeTable: {
            if (!operation.HasCreateTable()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE TABLE scheme operation");
            }
            const auto& create = operation.GetCreateTable();
            if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateTable
                && create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateIndexedTable)
            {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE TABLE modify scheme operation");
            }
            return FinishPrepared(result->Status, create);
        }
        case NKikimrSchemeOp::EPathTypeView: {
            if (!operation.HasCreateView()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE VIEW scheme operation");
            }
            const auto& create = operation.GetCreateView();
            if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateView) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE VIEW modify scheme operation");
            }
            return FinishPrepared(result->Status, create);
        }
        case NKikimrSchemeOp::EPathTypeReplication: {
            if (!operation.HasCreateReplication()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE ASYNC REPLICATION scheme operation");
            }
            const auto& create = operation.GetCreateReplication();
            if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateReplication) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE ASYNC REPLICATION modify scheme operation");
            }
            return FinishPrepared(result->Status, create);
        }
        case NKikimrSchemeOp::EPathTypeTransfer: {
            if (!operation.HasCreateTransfer()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE TRANSFER scheme operation");
            }
            const auto& create = operation.GetCreateTransfer();
            if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateTransfer) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE TRANSFER modify scheme operation");
            }
            return FinishPrepared(result->Status, create);
        }
        case NKikimrSchemeOp::EPathTypeExternalDataSource: {
            if (!operation.HasCreateExternalDataSource()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE EXTERNAL DATA SOURCE scheme operation");
            }
            const auto& create = operation.GetCreateExternalDataSource();
            if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateExternalDataSource) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE EXTERNAL DATA SOURCE modify scheme operation");
            }
            return FinishPrepared(result->Status, create);
        }
        case NKikimrSchemeOp::EPathTypeExternalTable: {
            if (!operation.HasCreateExternalTable()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE EXTERNAL TABLE scheme operation");
            }
            const auto& create = operation.GetCreateExternalTable();
            if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateExternalTable) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE EXTERNAL TABLE modify scheme operation");
            }
            return FinishPrepared(result->Status, create);
        }
        default:
            return Finish(Ydb::StatusIds::GENERIC_ERROR,
                "missing persisted creation-query path type; restart the import");
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, std::variant<TString, NKikimrSchemeOp::TModifyScheme> result) {
        auto logMessage = TStringBuilder() << "TSchemeQueryExecutor Reply"
            << ", self: " << SelfId()
            << ", status: " << status;
        LOG_I(logMessage);

        std::visit([&]<typename T>(T& value) {
            if constexpr (std::is_same_v<T, TString>) {
                logMessage << ", error: " << value;
            } else if constexpr (std::is_same_v<T, NKikimrSchemeOp::TModifyScheme>) {
                logMessage << ", prepared query: " << value.ShortDebugString().Quote();
            }
            LOG_D(logMessage);
            Send(ReplyTo, new TEvPrivate::TEvImportSchemeQueryResult(ImportId, ItemIdx, status, std::move(value)));
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
        NKikimrSchemeOp::EPathType creationQueryPathType,
        const TString& destinationPath,
        const TString& database
    )
        : ReplyTo(replyTo)
        , ImportId(importId)
        , ItemIdx(itemIdx)
        , SchemeQuery(schemeQuery)
        , CreationQueryPathType(creationQueryPathType)
        , DestinationPath(destinationPath)
        , Database(database)
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
    NKikimrSchemeOp::EPathType CreationQueryPathType;
    TString DestinationPath;
    TString Database;

    // The following pointer-type event arguments are necessary for constructing the compile request.
    // These pointers must remain valid until the compilation response is received.
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TGUCSettings::TPtr GUCSettings;
    std::shared_ptr<std::atomic<bool>> IsInterestedInResult;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;

}; // TSchemeQueryExecutor

IActor* CreateSchemeQueryExecutor(
    NActors::TActorId replyTo,
    ui64 importId,
    ui32 itemIdx,
    const TString& schemeQuery,
    NKikimrSchemeOp::EPathType creationQueryPathType,
    const TString& destinationPath,
    const TString& database)
{
    return new TSchemeQueryExecutor(
        replyTo,
        importId,
        itemIdx,
        schemeQuery,
        creationQueryPathType,
        destinationPath,
        database);
}

} // NKikimr::NSchemeShard

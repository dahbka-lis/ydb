#pragma once

#include <ydb/core/protos/import.pb.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ylimits.h>

namespace NKikimr::NSchemeShard {

NActors::IActor* CreateSchemeQueryExecutor(
    NActors::TActorId replyTo,
    ui64 importId,
    ui32 itemIdx,
    const TString& creationQuery,
    const TString& database,
    NKikimrImport::TTableSchemaImportPlan::EStatementKind expectedKind =
        NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_UNSPECIFIED,
    ui32 statementOrdinal = Max<ui32>(),
    ui64 preparationAttempt = 0);

}

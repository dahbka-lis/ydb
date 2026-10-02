#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSchemeShard {

enum class EImportSchemeQueryKind {
    Other,
    Table,
};

NActors::IActor* CreateSchemeQueryExecutor(
    NActors::TActorId replyTo,
    ui64 importId,
    ui32 itemIdx,
    const TString& creationQuery,
    const TString& database,
    EImportSchemeQueryKind queryKind = EImportSchemeQueryKind::Other);

}

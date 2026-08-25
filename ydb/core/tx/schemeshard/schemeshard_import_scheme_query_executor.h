#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimrSchemeOp {
enum EPathType : int;
class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

TMaybe<NKikimrSchemeOp::EPathType> GetPreparedQueryPathType(
    const NKikimrSchemeOp::TModifyScheme& modifyScheme);

bool ValidatePreparedQueryOperation(
    const NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::EPathType expectedPathType,
    TString& error);

TMaybe<TString> GetPreparedQueryTargetPath(
    const NKikimrSchemeOp::TModifyScheme& modifyScheme);

NActors::IActor* CreateSchemeQueryExecutor(
    NActors::TActorId replyTo,
    ui64 importId,
    ui32 itemIdx,
    const TString& creationQuery,
    NKikimrSchemeOp::EPathType creationQueryPathType,
    const TString& destinationPath,
    const TString& database);

}

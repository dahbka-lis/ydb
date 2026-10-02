#pragma once

#include <util/generic/string.h>

namespace NKikimrSchemeOp {
class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TImportInfo;

bool PrepareTableCreationQuery(
    const TString& script,
    const TString& destinationPath,
    TString& creationQuery,
    TString& error);

bool NormalizeTableCreationForImport(
    TSchemeShard* ss,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    NKikimrSchemeOp::TModifyScheme& prepared,
    TString& error);

} // namespace NKikimr::NSchemeShard

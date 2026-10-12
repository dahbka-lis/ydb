#pragma once

#include <ydb/core/protos/import.pb.h>

#include <util/generic/string.h>

namespace NYql {
class TIssues;
}

namespace NKikimr::NSchemeShard {

struct TImportInfo;

bool PrepareTableImportSql(
    const TString& sql,
    const TString& destinationDatabase,
    const TString& destinationPath,
    NKikimrImport::TTableSchemaImportPlan& plan,
    NYql::TIssues& issues);

bool NormalizeSqlTableCreate(
    NKikimrSchemeOp::TModifyScheme& operation,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error);

bool ValidateSqlTableOperation(
    const NKikimrSchemeOp::TModifyScheme& operation,
    const NKikimrImport::TTableSchemaImportPlan::TStatement& statement,
    TString& error);

} // namespace NKikimr::NSchemeShard

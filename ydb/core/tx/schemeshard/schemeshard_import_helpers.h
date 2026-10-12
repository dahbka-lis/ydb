#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/public/api/protos/ydb_import.pb.h>

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NSchemeShard {

template <typename T>
void AddIssue(T& response, const TString& message, NYql::TSeverityIds::ESeverityId severity = NYql::TSeverityIds::S_ERROR) {
    auto& issue = *response.AddIssues();
    issue.set_severity(severity);
    issue.set_message(message);
}

struct TImportInfo;

TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx, i32 indexIdx);
TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx);

bool NeedToBuildIndexes(const TImportInfo& importInfo, ui32 itemIdx);
bool IsLocalIndexType(NKikimrSchemeOp::EIndexType type);
bool ShouldRestoreIndexImplementationData(
    Ydb::Import::ImportFromS3Settings::IndexPopulationMode populationMode,
    bool backupIncomplete);

class TSchemeShard;

bool ValidateImportDstPath(const TString& dstPath, TSchemeShard* ss, TString& explain);

} // NKikimr::NSchemeShard

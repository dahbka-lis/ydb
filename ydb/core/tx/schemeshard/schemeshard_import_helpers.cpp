#include "schemeshard_import_helpers.h"
#include "schemeshard_info_types.h"

namespace NKikimr::NSchemeShard {

TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx, i32 indexIdx) {
    return TStringBuilder() << importInfo.Id << "-" << itemIdx << "-" << indexIdx;
}

TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    return MakeIndexBuildUid(importInfo, itemIdx, item.NextIndexIdx);
}

bool IsLocalIndexType(NKikimrSchemeOp::EIndexType type) {
    switch (type) {
        case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
        case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
        case NKikimrSchemeOp::EIndexTypeLocalMinMax:
        case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
            return true;
        default:
            return false;
    }
}

bool ShouldRestoreIndexImplementationData(
    Ydb::Import::ImportFromS3Settings::IndexPopulationMode populationMode,
    bool backupIncomplete)
{
    switch (populationMode) {
        case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT:
            return true;
        case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO:
            return !backupIncomplete;
        case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_BUILD:
        case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_UNSPECIFIED:
            return false;
    }
    return false;
}

} // NKikimr::NSchemeShard

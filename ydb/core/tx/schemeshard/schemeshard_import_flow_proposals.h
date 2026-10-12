#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

NKikimrSchemeOp::TTableDescription MakeColumnTableDataLayout(
    const NKikimrSchemeOp::TColumnTableDescription& columnTable
);

bool RebuildTableDescriptionForImport(
    const NKikimrSchemeOp::TTableDescription& destination,
    const Ydb::Table::CreateTableRequest& backupLayout,
    NKikimrSchemeOp::TTableDescription& restoreLayout,
    TString& error
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> RestoreTableDataPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

THolder<TEvSchemeShard::TEvCancelTx> CancelRestoreTableDataPropose(
    const TImportInfo& importInfo,
    TTxId restoreTxId
);

THolder<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    const TString& uid,
    TString& error
);

THolder<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    const TImportInfo& importInfo,
    TTxId indexBuildId
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateChangefeedPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    const TImportInfo::TItem& item,
    TString& error
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateConsumersPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    TImportInfo::TItem& item
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTopicPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

} // NSchemeShard
} // NKikimr

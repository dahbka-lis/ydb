#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_import_helpers.h"

#include "schemeshard_path_describer.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/persqueue/public/schema/schema_propose.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <google/protobuf/util/time_util.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {

enum class EPreparedIndexKind {
    Global,
    GlobalAsync,
    GlobalUnique,
    Local,
    Unsupported,
};

EPreparedIndexKind ClassifyPreparedIndex(const NKikimrSchemeOp::TIndexCreationConfig& index) {
    switch (NTableIndex::GetIndexType(index)) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
            return EPreparedIndexKind::Global;
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            return EPreparedIndexKind::GlobalAsync;
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            return EPreparedIndexKind::GlobalUnique;
        case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
        case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
        case NKikimrSchemeOp::EIndexTypeLocalMinMax:
        case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
            return EPreparedIndexKind::Local;
        default:
            return EPreparedIndexKind::Unsupported;
    }
}

bool IsBuildablePreparedIndex(EPreparedIndexKind kind) {
    return IsIn({
        EPreparedIndexKind::Global,
        EPreparedIndexKind::GlobalAsync,
        EPreparedIndexKind::GlobalUnique,
    }, kind);
}

const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TIndexCreationConfig>* GetPreparedIndexes(
    const TImportInfo::TItem& item,
    TString& error)
{
    if (!item.PreparedCreationQuery ||
        item.PreparedCreationQuery->GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
        return nullptr;
    }

    if (!item.PreparedCreationQuery->HasCreateIndexedTable()) {
        error = "Prepared CREATE INDEXED TABLE operation has no creation config";
        return nullptr;
    }

    return &item.PreparedCreationQuery->GetCreateIndexedTable().GetIndexDescription();
}

bool FillPreparedIndexDescription(
    Ydb::Table::TableIndex& index,
    const NKikimrSchemeOp::TIndexCreationConfig& preparedIndex,
    TString& error)
{
    if (preparedIndex.GetName().empty()) {
        error = "Prepared index has no name";
        return false;
    }

    index.set_name(preparedIndex.GetName());
    index.mutable_index_columns()->CopyFrom(preparedIndex.GetKeyColumnNames());
    index.mutable_data_columns()->CopyFrom(preparedIndex.GetDataColumnNames());

    switch (ClassifyPreparedIndex(preparedIndex)) {
        case EPreparedIndexKind::Global:
            index.mutable_global_index();
            return true;
        case EPreparedIndexKind::GlobalAsync:
            index.mutable_global_async_index();
            return true;
        case EPreparedIndexKind::GlobalUnique:
            index.mutable_global_unique_index();
            return true;
        default:
            error = TStringBuilder()
                << "Unsupported prepared index type: "
                << NKikimrSchemeOp::EIndexType_Name(NTableIndex::GetIndexType(preparedIndex));
            return false;
    }
}

} // anonymous namespace

bool IsLocalPreparedIndex(const NKikimrSchemeOp::TIndexCreationConfig& index) {
    return ClassifyPreparedIndex(index) == EPreparedIndexKind::Local;
}

bool ValidatePreparedIndexes(
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error)
{
    error.clear();
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    if (!item.PreparedCreationQuery) {
        return true;
    }

    const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TIndexCreationConfig>* indexes = nullptr;
    switch (item.PreparedCreationQuery->GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateTable:
            break;
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
            indexes = GetPreparedIndexes(item, error);
            if (!indexes) {
                return false;
            }
            break;
        default:
            return true;
    }

    const bool buildIndexes = NeedToBuildIndexes(importInfo, itemIdx);

    THashSet<TString> expectedPaths;
    TVector<TString> expectedPathsInOrder;
    if (indexes) {
        for (const auto& index : *indexes) {
            const auto kind = ClassifyPreparedIndex(index);
            if (kind == EPreparedIndexKind::Local) {
                continue;
            }
            if (!IsBuildablePreparedIndex(kind)) {
                error = TStringBuilder()
                    << "Unsupported prepared index type: "
                    << NKikimrSchemeOp::EIndexType_Name(NTableIndex::GetIndexType(index));
                return false;
            }
            if (index.GetName().empty()) {
                error = "Prepared index has no name";
                return false;
            }

            if (buildIndexes) {
                continue;
            }

            const TVector<TString> indexColumns(
                index.GetKeyColumnNames().begin(), index.GetKeyColumnNames().end());
            for (const auto implTable :
                 NTableIndex::GetImplTables(NTableIndex::GetIndexType(index), indexColumns)) {
                TString expectedPath = TStringBuilder()
                    << item.DstPathName << "/" << index.GetName() << "/" << implTable;
                if (!expectedPaths.insert(expectedPath).second) {
                    error = TStringBuilder()
                        << "Duplicate expected materialized index child path: " << expectedPath;
                    return false;
                }
                expectedPathsInOrder.push_back(std::move(expectedPath));
            }
        }
    }

    if (buildIndexes) {
        return true;
    }

    // AUTO with no usable materialized children is BUILD. A nonempty ChildItems set,
    // however, has already been persisted by the scheme getter. It cannot be discarded
    // here without racing or duplicating implementation-table restores, so validate it
    // exactly like explicit IMPORT and fail before creating the destination on mismatch.

    THashSet<TString> materializedPaths;
    TVector<TString> materializedPathsInOrder;
    materializedPaths.reserve(item.ChildItems.size());
    materializedPathsInOrder.reserve(item.ChildItems.size());
    for (const ui32 childIdx : item.ChildItems) {
        if (childIdx >= importInfo.Items.size()) {
            error = TStringBuilder()
                << "Materialized index child position is out of range: " << childIdx;
            return false;
        }

        const auto& materializedPath = importInfo.Items.at(childIdx).DstPathName;
        if (!materializedPaths.insert(materializedPath).second) {
            error = TStringBuilder()
                << "Duplicate materialized index child path: " << materializedPath;
            return false;
        }
        materializedPathsInOrder.push_back(materializedPath);
    }

    for (const auto& materializedPath : materializedPathsInOrder) {
        if (!expectedPaths.contains(materializedPath)) {
            error = TStringBuilder()
                << "Unexpected materialized index child table: "
                << materializedPath;
            return false;
        }
    }

    for (const auto& expectedPath : expectedPathsInOrder) {
        if (!materializedPaths.contains(expectedPath)) {
            error = TStringBuilder()
                << "Missing materialized index child table: " << expectedPath;
            return false;
        }
    }

    return true;
}

bool PrepareNextBuildableIndex(
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TImportInfo::TItem& item,
    TString& error)
{
    error.clear();
    if (!NeedToBuildIndexes(importInfo, itemIdx)) {
        return false;
    }

    if (item.Table) {
        while (item.NextIndexIdx < item.Table->indexes_size() &&
               NTableIndex::IsLocalTableIndex(item.Table->indexes(item.NextIndexIdx).type_case())) {
            ++item.NextIndexIdx;
        }

        return item.NextIndexIdx < item.Table->indexes_size();
    }

    const auto* indexes = GetPreparedIndexes(item, error);
    if (!indexes) {
        return false;
    }

    while (item.NextIndexIdx < indexes->size() &&
           IsLocalPreparedIndex(indexes->Get(item.NextIndexIdx))) {
        ++item.NextIndexIdx;
    }

    if (item.NextIndexIdx >= indexes->size()) {
        return false;
    }

    Ydb::Table::TableIndex unused;
    return FillPreparedIndexDescription(unused, indexes->Get(item.NextIndexIdx), error);
}

static bool FillDefaultValues(
    const NKikimr::NSchemeShard::TImportInfo::TItem& item,
    ::NKikimrSchemeOp::TIndexedTableCreationConfig& indexedTable,
    TString& error)
{
    for (const auto& column : item.Table->columns()) {
        switch (column.default_value_case()) {
            case Ydb::Table::ColumnMeta::kFromSequence: {
                const auto& fromSequence = column.from_sequence();
                Ydb::StatusIds::StatusCode status;
                auto* seqDesc = indexedTable.MutableSequenceDescription()->Add();
                if (!FillSequenceDescription(*seqDesc, fromSequence, status, error)) {
                    return false;
                }

                break;
            }
            case Ydb::Table::ColumnMeta::kFromLiteral:
                break;
            default:
                break;
        }
    }
    return true;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    Y_ABORT_UNLESS(item.Table);

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    const bool isColumnTable = item.Table->store_type() == Ydb::Table::STORE_TYPE_COLUMN;
    modifyScheme.SetOperationType(isColumnTable ? NKikimrSchemeOp::ESchemeOpCreateColumnTable : NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);

    std::pair<TString, TString> wdAndPath;
    if (!TrySplitPathByDb(item.DstPathName, domainPath.PathString(), wdAndPath, error)) {
        return nullptr;
    }

    modifyScheme.SetWorkingDir(wdAndPath.first);

    if (isColumnTable) {
        auto& tableDesc = *modifyScheme.MutableCreateColumnTable();
        tableDesc.SetName(wdAndPath.second);
        tableDesc.SetIsRestore(true);

        Y_ABORT_UNLESS(ss->TableProfilesLoaded);
        Ydb::StatusIds::StatusCode status;
        if (!FillColumnTableDescription(modifyScheme, *item.Table, status, error)) {
            return nullptr;
        }
    } else {
        auto& indexedTable = *modifyScheme.MutableCreateIndexedTable();
        auto& tableDesc = *indexedTable.MutableTableDescription();
        tableDesc.SetName(wdAndPath.second);
        tableDesc.SetIsRestore(true);

        Y_ABORT_UNLESS(ss->TableProfilesLoaded);
        Ydb::StatusIds::StatusCode status;
        if (!FillTableDescription(modifyScheme, *item.Table, ss->TableProfiles, status, error, true)) {
            return nullptr;
        }

        if (!NeedToBuildIndexes(importInfo, itemIdx) && !FillIndexDescription(indexedTable, *item.Table, status, error)) {
            return nullptr;
        }

        if (!FillDefaultValues(item, indexedTable, error)) {
            return nullptr;
        }
    }

    if (importInfo.UserSID) {
        record.SetOwner(*importInfo.UserSID);
    }
    FillOwner(record, item.Permissions);

    record.SetOwner(ChooseAppropriateOwner(record, AppData()));

    if (!FillACL(modifyScheme, item.Permissions, error)) {
        return nullptr;
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
) {
    TString unused;
    return CreateTablePropose(ss, txId, importInfo, itemIdx, unused);
}

template <typename TPath>
static auto GetDescription(TSchemeShard* ss, const TPath& path) {
    NKikimrSchemeOp::TDescribeOptions opts;
    opts.SetShowPrivateTable(true);
    return DescribePath(ss, TlsActivationContext->AsActorContext(), path, opts);
}

static NKikimrSchemeOp::TTableDescription GetTableDescription(TSchemeShard* ss, const TPathId& pathId) {
    auto desc = GetDescription(ss, pathId);
    auto record = desc->GetRecord();

    Y_ABORT_UNLESS(record.HasPathDescription());
    const auto& pathDesc = record.GetPathDescription();
    Y_ABORT_UNLESS(pathDesc.HasTable() || pathDesc.HasColumnTableDescription());

    if (pathDesc.HasColumnTableDescription()) {
        NKikimrSchemeOp::TTableDescription result;
        const auto& columnTable = pathDesc.GetColumnTableDescription();
        THashMap<TString, ui32> columnIds;
        for (const auto& column : columnTable.GetSchema().GetColumns()) {
            auto& dstColumn = *result.add_columns();
            dstColumn.set_name(column.GetName());
            dstColumn.set_type(column.GetType());
            dstColumn.set_typeid_(column.GetTypeId());
            dstColumn.set_id(column.GetId());
            columnIds[column.GetName()] = column.GetId();
        }

        result.MutableKeyColumnNames()->CopyFrom(columnTable.GetSchema().GetKeyColumnNames());
        for (const auto& keyColumnName : columnTable.GetSchema().GetKeyColumnNames()) {
            auto it = columnIds.find(keyColumnName);
            Y_ABORT_UNLESS(it != columnIds.end());
            result.AddKeyColumnIds(it->second);
        }

        result.MutablePartitionConfig()->MutablePartitioningPolicy()->SetMinPartitionsCount(columnTable.GetColumnShardCount());
        return result;
    }

    return record.GetPathDescription().GetTable();
}

static NKikimrSchemeOp::TTableDescription RebuildTableDescription(
    const NKikimrSchemeOp::TTableDescription& src,
    const Ydb::Table::CreateTableRequest& scheme
) {
    NKikimrSchemeOp::TTableDescription tableDesc;
    tableDesc.MutableKeyColumnNames()->CopyFrom(src.GetKeyColumnNames());
    tableDesc.MutableKeyColumnIds()->CopyFrom(src.GetKeyColumnIds());

    THashMap<TString, ui32> columnNameToIdx;
    for (ui32 i = 0; i < src.ColumnsSize(); ++i) {
        Y_ABORT_UNLESS(columnNameToIdx.emplace(src.GetColumns(i).GetName(), i).second);
    }

    for (const auto& column : scheme.columns()) {
        auto it = columnNameToIdx.find(column.name());
        Y_ABORT_UNLESS(it != columnNameToIdx.end());

        Y_ABORT_UNLESS(it->second < src.ColumnsSize());
        tableDesc.MutableColumns()->Add()->CopyFrom(src.GetColumns(it->second));
    }

    for (const auto& stat : scheme.statistics()) {
        FillMultiColumnStatistics(*tableDesc.AddMultiColumnStatistics(), stat);
    }

    return tableDesc;
}

template <typename TSettings>
void FillRestoreEncryptionSettings(
    NKikimrSchemeOp::TRestoreTask& task,
    const TSettings& settings,
    const TImportInfo::TItem& item)
{
    if (settings.has_encryption_settings()) {
        auto& taskEncryptionSettings = *task.MutableEncryptionSettings();
        *taskEncryptionSettings.MutableSymmetricKey() = settings.encryption_settings().symmetric_key();
        if (item.ExportItemIV) {
            taskEncryptionSettings.SetIV(item.ExportItemIV->GetBinaryString());
        }
    }
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> RestoreTableDataPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRestore);
    modifyScheme.SetInternal(true);

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    Y_ABORT_UNLESS(dstPath.IsResolved());

    modifyScheme.SetWorkingDir(dstPath.Parent().PathString());

    auto& task = *modifyScheme.MutableRestore();
    task.SetTableName(dstPath.LeafName());
    const auto destination = GetTableDescription(ss, item.DstPathId);
    if (item.Table) {
        *task.MutableTableDescription() = RebuildTableDescription(destination, *item.Table);
    } else {
        auto tableDescription = destination;
        tableDescription.ClearColumns();
        for (const auto& column : destination.GetColumns()) {
            if (column.HasDefaultFromExpression() && !column.GetDefaultFromExpression().GetStored()) {
                continue;
            }
            tableDescription.AddColumns()->CopyFrom(column);
        }
        *task.MutableTableDescription() = std::move(tableDescription);
    }

    switch (importInfo.Kind) {
    case TImportInfo::EKind::S3:
        {
            auto settings = importInfo.GetS3Settings();
            FillRestoreEncryptionSettings(task, settings, item);
            task.SetNumberOfRetries(settings.number_of_retries());
            auto& restoreSettings = *task.MutableS3Settings();
            restoreSettings.SetEndpoint(settings.endpoint());
            restoreSettings.SetBucket(settings.bucket());
            restoreSettings.SetAccessKey(settings.access_key());
            restoreSettings.SetSecretKey(settings.secret_key());
            restoreSettings.SetObjectKeyPattern(importInfo.GetItemSrcPrefix(itemIdx));
            restoreSettings.SetUseVirtualAddressing(!settings.disable_virtual_addressing());

            switch (settings.scheme()) {
            case Ydb::Import::ImportFromS3Settings::HTTP:
                restoreSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
                break;
            case Ydb::Import::ImportFromS3Settings::HTTPS:
                restoreSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTPS);
                break;
            default:
                Y_ABORT("Unknown scheme");
            }

            if (const auto region = settings.region()) {
                restoreSettings.SetRegion(region);
            }
        }
        break;

    case TImportInfo::EKind::FS:
        {
            auto settings = importInfo.GetFsSettings();
            FillRestoreEncryptionSettings(task, settings, item);
            task.SetNumberOfRetries(settings.number_of_retries());
            auto& restoreSettings = *task.MutableFSSettings();
            restoreSettings.SetBasePath(settings.base_path());
            restoreSettings.SetPath(importInfo.GetItemSrcPrefix(itemIdx));
        }
        break;
    }

    const auto* metadata = &item.Metadata;
    if (item.ParentIdx != Max<ui32>()) {
        Y_ABORT_UNLESS(item.ParentIdx < importInfo.Items.size());
        metadata = &importInfo.Items[item.ParentIdx].Metadata;
    }

    if (!metadata->HasVersion() || metadata->GetVersion() > 0) {
        task.SetValidateChecksums(!importInfo.GetSkipChecksumValidation());
    }

    return propose;
}

THolder<TEvSchemeShard::TEvCancelTx> CancelRestoreTableDataPropose(
    const TImportInfo& importInfo,
    TTxId restoreTxId
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvCancelTx>();

    auto& record = propose->Record;
    record.SetTxId(importInfo.Id);
    record.SetTargetTxId(ui64(restoreTxId));

    return propose;
}

THolder<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    const TString& uid,
    TString& error
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);

    NKikimrIndexBuilder::TIndexBuildSettings settings;

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    settings.set_source_path(dstPath.PathString());
    if (ss->MaxRestoreBuildIndexShardsInFlight) {
        settings.set_max_shards_in_flight(ss->MaxRestoreBuildIndexShardsInFlight);
    }

    if (item.Table) {
        if (item.NextIndexIdx >= item.Table->indexes_size()) {
            error = "Index position is out of range for the imported table schema";
            return nullptr;
        }

        settings.mutable_index()->CopyFrom(item.Table->indexes(item.NextIndexIdx));
        if (settings.mutable_index()->type_case() == Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET) {
            settings.mutable_index()->mutable_global_index();
        }
    } else {
        const auto* indexes = GetPreparedIndexes(item, error);
        if (!indexes) {
            if (error.empty()) {
                error = "Prepared CREATE INDEXED TABLE operation is required to build indexes";
            }
            return nullptr;
        }
        if (item.NextIndexIdx >= indexes->size()) {
            error = "Index position is out of range for the prepared table schema";
            return nullptr;
        }
        if (!FillPreparedIndexDescription(
                *settings.mutable_index(), indexes->Get(item.NextIndexIdx), error)) {
            return nullptr;
        }
    }

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    auto propose = MakeHolder<TEvIndexBuilder::TEvCreateRequest>(ui64(txId), domainPath.PathString(), std::move(settings));
    auto& request = propose->Record;
    (*request.MutableOperationParams()->mutable_labels())["uid"] = uid;
    request.SetInternal(true);

    return propose;
}

THolder<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    const TImportInfo& importInfo,
    TTxId indexBuildId
) {
    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    return MakeHolder<TEvIndexBuilder::TEvCancelRequest>(ui64(indexBuildId), domainPath.PathString(), ui64(indexBuildId));
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateChangefeedPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    const TImportInfo::TItem& item,
    TString& error
) {
    Y_ABORT_UNLESS(item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size());

    const auto& importChangefeedTopic = item.Changefeeds.GetChangefeeds()[item.NextChangefeedIdx];
    const auto& changefeed = importChangefeedTopic.GetChangefeed();
    const auto& topic = importChangefeedTopic.GetTopic();

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream);
    auto& cdcStream = *modifyScheme.MutableCreateCdcStream();

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    modifyScheme.SetWorkingDir(dstPath.Parent().PathString());
    cdcStream.SetTableName(dstPath.LeafName());

    auto& cdcStreamDescription = *cdcStream.MutableStreamDescription();
    Ydb::StatusIds::StatusCode status;
    if (!FillChangefeedDescription(cdcStreamDescription, changefeed, status, error)) {
        return nullptr;
    }

    if (topic.has_retention_period()) {
        cdcStream.SetRetentionPeriodSeconds(topic.retention_period().seconds());
    }

    auto tableDesc = GetTableDescription(ss, dstPath->PathId);
    Y_ABORT_UNLESS(!tableDesc.GetKeyColumnIds().empty());
    const auto& keyId = tableDesc.GetKeyColumnIds()[0];
    bool isPartitioningAvailable = false;

    // Explicit specification of the number of partitions when creating CDC
    // is possible only if the first component of the primary key
    // of the source table is Uint32 or Uint64
    for (const auto& column : tableDesc.GetColumns()) {
        if (column.GetId() == keyId) {
            isPartitioningAvailable = column.GetType() == "Uint32" || column.GetType() == "Uint64";
            break;
        }
    }

    if (topic.has_partitioning_settings()) {
        if (isPartitioningAvailable) {
            i64 minActivePartitions =
                topic.partitioning_settings().min_active_partitions();
            if (minActivePartitions < 0) {
                error = "minActivePartitions must be >= 0";
                return nullptr;
            } else if (minActivePartitions == 0) {
                minActivePartitions = 1;
            }
            cdcStream.SetTopicPartitions(minActivePartitions);
        }

        if (topic.partitioning_settings().has_auto_partitioning_settings()) {
            auto& partitioningSettings = topic.partitioning_settings().auto_partitioning_settings();
            cdcStream.SetTopicAutoPartitioning(partitioningSettings.strategy() != ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);

            i64 maxActivePartitions =
                topic.partitioning_settings().max_active_partitions();
            if (maxActivePartitions < 0) {
                error = "maxActivePartitions must be >= 0";
                return nullptr;
            } else if (maxActivePartitions == 0) {
                maxActivePartitions = 50;
            }
            cdcStream.SetMaxPartitionCount(maxActivePartitions);
        }
    }
    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateConsumersPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    TImportInfo::TItem& item
) {
    using google::protobuf::util::TimeUtil;

    Y_ABORT_UNLESS(item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size());

    const auto& importChangefeedTopic = item.Changefeeds.GetChangefeeds()[item.NextChangefeedIdx];
    const auto& topic = importChangefeedTopic.GetTopic();

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    auto& pqGroup = *modifyScheme.MutableAlterPersQueueGroup();

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    const TString changefeedPath = dstPath.PathString() + "/" + importChangefeedTopic.GetChangefeed().name();
    modifyScheme.SetWorkingDir(changefeedPath);
    modifyScheme.SetInternal(true);

    pqGroup.SetName("streamImpl");

    auto describeSchemeResult = GetDescription(ss, changefeedPath + "/streamImpl");

    const auto& response = describeSchemeResult->GetRecord().GetPathDescription();
    item.StreamImplPathId = {response.GetSelf().GetSchemeshardId(), response.GetSelf().GetPathId()};
    pqGroup.CopyFrom(response.GetPersQueueGroup());

    pqGroup.ClearTotalGroupCount();
    pqGroup.MutablePQTabletConfig()->ClearPartitionKeySchema();

    auto* tabletConfig = pqGroup.MutablePQTabletConfig();
    const auto& pqConfig = AppData()->PQConfig;

    for (const auto& consumer : topic.consumers()) {
        auto& addedConsumer = *tabletConfig->AddConsumers();
        auto consumerName = NPersQueue::ConvertNewConsumerName(consumer.name(), pqConfig);
        addedConsumer.SetName(consumerName);
        if (consumer.important()) {
            addedConsumer.SetImportant(true);
        }

        addedConsumer.SetAvailabilityPeriodMs(TimeUtil::DurationToMilliseconds(consumer.availability_period()));
        if (consumer.has_shared_consumer_type()) {
            const auto& sharedConsumerType = consumer.shared_consumer_type();
            addedConsumer.SetType(::NKikimrPQ::TPQTabletConfig_EConsumerType::TPQTabletConfig_EConsumerType_CONSUMER_TYPE_MLP);
            addedConsumer.SetKeepMessageOrder(sharedConsumerType.keep_messages_order());
            addedConsumer.SetDefaultProcessingTimeoutSeconds(TimeUtil::DurationToSeconds(sharedConsumerType.default_processing_timeout()));
            addedConsumer.SetDefaultDelayMessageTimeMs(TimeUtil::DurationToMilliseconds(sharedConsumerType.receive_message_delay()));
            addedConsumer.SetDefaultReceiveMessageWaitTimeMs(TimeUtil::DurationToMilliseconds(sharedConsumerType.receive_message_wait_time()));
            const auto& deadLetterPolicy = sharedConsumerType.dead_letter_policy();

            if (sharedConsumerType.has_dead_letter_policy() && deadLetterPolicy.enabled()) {
                const auto& deadLetterPolicy = sharedConsumerType.dead_letter_policy();
                addedConsumer.SetDeadLetterPolicyEnabled(true);
                if (deadLetterPolicy.has_condition()) {
                    addedConsumer.SetMaxProcessingAttempts(deadLetterPolicy.condition().max_processing_attempts());
                }

                if (deadLetterPolicy.has_move_action()) {
                    addedConsumer.SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig_EDeadLetterPolicy::TPQTabletConfig_EDeadLetterPolicy_DEAD_LETTER_POLICY_MOVE);
                    addedConsumer.SetDeadLetterQueue(deadLetterPolicy.move_action().dead_letter_queue());
                } else if (deadLetterPolicy.has_delete_action()) {
                    addedConsumer.SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig_EDeadLetterPolicy::TPQTabletConfig_EDeadLetterPolicy_DEAD_LETTER_POLICY_DELETE);
                } else {
                    addedConsumer.SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig_EDeadLetterPolicy::TPQTabletConfig_EDeadLetterPolicy_DEAD_LETTER_POLICY_UNSPECIFIED);
                }
            } else {
                addedConsumer.SetDeadLetterPolicyEnabled(false);
            }
        } else {
            addedConsumer.SetType(::NKikimrPQ::TPQTabletConfig_EConsumerType::TPQTabletConfig_EConsumerType_CONSUMER_TYPE_STREAMING);
        }
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTopicPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    Y_ABORT_UNLESS(item.Topic);

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    const TString database = domainPath.PathString();

    std::pair<TString, TString> wdAndPath;
    if (!TrySplitPathByDb(item.DstPathName, database, wdAndPath, error)) {
        return nullptr;
    }

    const auto& [workingDir, name] = wdAndPath;

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;
    auto& modifyScheme = *record.AddTransaction();

    if (auto result = NPQ::NSchema::ProposeCreateTopic(modifyScheme, *item.Topic, database, workingDir, name); !result) {
        error = std::move(result.GetErrorMessage());
        return nullptr;
    }

    return propose;
}

} // NSchemeShard
} // NKikimr

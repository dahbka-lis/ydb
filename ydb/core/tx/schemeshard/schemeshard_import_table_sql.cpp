#include "schemeshard_import_table_sql.h"

#include "schemeshard_import_helpers.h"
#include "schemeshard_info_types.h"
#include "schemeshard_impl.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/algorithm.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {
namespace {

using namespace NSQLv1Generated;

template <typename TMessage>
void FindMessages(const NProtoBuf::Message& message, TVector<const TMessage*>& result) {
    const auto* descriptor = message.GetDescriptor();
    for (int fieldIdx = 0; fieldIdx < descriptor->field_count(); ++fieldIdx) {
        NProtoBuf::TConstField field(message, descriptor->field(fieldIdx));
        if (!field.IsMessage()) {
            continue;
        }

        for (size_t valueIdx = 0; valueIdx < field.Size(); ++valueIdx) {
            const auto& child = *field.Get<NProtoBuf::Message>(valueIdx);
            if (const auto* typed = dynamic_cast<const TMessage*>(&child)) {
                result.push_back(typed);
            }
            FindMessages(child, result);
        }
    }
}

class TQueryRenderer {
public:
    TQueryRenderer(const NProtoBuf::Message* target, const TString& destinationPath)
        : Target(target)
        , DestinationPath(destinationPath)
    {
    }

    TString Render(const NProtoBuf::Message& message) {
        Visit(message);
        return Stream.Str();
    }

private:
    void Delimit() {
        if (HasTokens) {
            Stream << ' ';
        }
        HasTokens = true;
    }

    void Visit(const NProtoBuf::Message& message) {
        if (&message == Target) {
            Delimit();
            NYql::EscapeArbitraryAtom(DestinationPath, '`', &Stream);
            return;
        }

        if (const auto* token = dynamic_cast<const TToken*>(&message)) {
            if (token->GetValue() != "<EOF>") {
                Delimit();
                Stream << token->GetValue();
            }
            return;
        }

        const auto* descriptor = message.GetDescriptor();
        for (int fieldIdx = 0; fieldIdx < descriptor->field_count(); ++fieldIdx) {
            NProtoBuf::TConstField field(message, descriptor->field(fieldIdx));
            if (!field.IsMessage()) {
                continue;
            }

            for (size_t valueIdx = 0; valueIdx < field.Size(); ++valueIdx) {
                Visit(*field.Get<NProtoBuf::Message>(valueIdx));
            }
        }
    }

private:
    const NProtoBuf::Message* const Target;
    const TString& DestinationPath;
    TStringStream Stream;
    bool HasTokens = false;
};

bool Fail(TString& creationQuery, TString& error, TStringBuf message) {
    creationQuery.clear();
    error = message;
    return false;
}

bool Fail(TString& error, TStringBuf message) {
    error = message;
    return false;
}

template <typename TRepeatedStrings>
TVector<TString> CopyStrings(const TRepeatedStrings& values) {
    return {values.begin(), values.end()};
}

template <typename TColumn>
TString ColumnType(const TColumn& column) {
    TStringBuilder type;
    type << column.GetType() << '#' << column.GetTypeId() << '#' << column.GetNotNull();
    if (column.HasTypeInfo()) {
        type << '#' << column.GetTypeInfo().SerializeAsString();
    }
    return type;
}

template <typename TColumn>
bool BuildColumnTypes(
        const google::protobuf::RepeatedPtrField<TColumn>& columns,
        THashMap<TString, TString>& result,
        TString& error)
{
    for (const auto& column : columns) {
        if (!result.emplace(column.GetName(), ColumnType(column)).second) {
            return Fail(error, TStringBuilder() << "duplicate table column: " << column.GetName());
        }
    }
    return true;
}

template <typename TLeftColumn, typename TRightColumn>
bool ValidateColumns(
        const google::protobuf::RepeatedPtrField<TLeftColumn>& sqlColumns,
        const google::protobuf::RepeatedPtrField<TRightColumn>& companionColumns,
        TString& error)
{
    THashMap<TString, TString> sql;
    THashMap<TString, TString> companion;
    if (!BuildColumnTypes(sqlColumns, sql, error) || !BuildColumnTypes(companionColumns, companion, error)) {
        return false;
    }
    if (sql.size() != companion.size()) {
        return Fail(error, TStringBuilder()
            << "SQL and companion schemas have different column counts: "
            << sql.size() << " and " << companion.size());
    }
    for (const auto& [name, type] : companion) {
        const auto it = sql.find(name);
        if (it == sql.end()) {
            return Fail(error, TStringBuilder() << "SQL schema is missing companion column: " << name);
        }
        if (it->second != type) {
            return Fail(error, TStringBuilder() << "SQL and companion column types differ for: " << name);
        }
    }
    return true;
}

bool IsLocalIndex(NKikimrSchemeOp::EIndexType type) {
    using enum NKikimrSchemeOp::EIndexType;
    return type == EIndexTypeLocalBloomFilter
        || type == EIndexTypeLocalBloomNgramFilter
        || type == EIndexTypeLocalMinMax
        || type == EIndexTypeLocalCountMinSketch;
}

struct TIndexShape {
    NKikimrSchemeOp::EIndexType Type = NKikimrSchemeOp::EIndexTypeInvalid;
    TVector<TString> Keys;
    TVector<TString> Data;

    bool operator==(const TIndexShape&) const = default;
};

bool BuildIndexShapes(
        const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TIndexCreationConfig>& indexes,
        THashMap<TString, TIndexShape>& result,
        TString& error)
{
    for (const auto& index : indexes) {
        TIndexShape shape{
            .Type = NTableIndex::GetIndexType(index),
            .Keys = CopyStrings(index.GetKeyColumnNames()),
            .Data = CopyStrings(index.GetDataColumnNames()),
        };
        if (!result.emplace(index.GetName(), std::move(shape)).second) {
            return Fail(error, TStringBuilder() << "duplicate table index: " << index.GetName());
        }
    }
    return true;
}

bool ValidateIndexes(
        const NKikimrSchemeOp::TIndexedTableCreationConfig& sql,
        const NKikimrSchemeOp::TIndexedTableCreationConfig& companion,
        TString& error)
{
    THashMap<TString, TIndexShape> sqlShapes;
    THashMap<TString, TIndexShape> companionShapes;
    if (!BuildIndexShapes(sql.GetIndexDescription(), sqlShapes, error)
        || !BuildIndexShapes(companion.GetIndexDescription(), companionShapes, error))
    {
        return false;
    }
    if (sqlShapes != companionShapes) {
        return Fail(error, "SQL and companion table index definitions differ");
    }
    return true;
}

bool ValidateSequenceMappings(
        const NKikimrSchemeOp::TIndexedTableCreationConfig& sql,
        const NKikimrSchemeOp::TIndexedTableCreationConfig& companion,
        TString& error)
{
    THashMap<TString, TString> sqlColumnSequences;
    for (const auto& column : sql.GetTableDescription().GetColumns()) {
        if (column.HasDefaultFromSequence()) {
            sqlColumnSequences.emplace(column.GetName(), column.GetDefaultFromSequence());
        }
    }

    THashMap<TString, TString> companionColumnSequences;
    for (const auto& column : companion.GetTableDescription().GetColumns()) {
        if (column.HasDefaultFromSequence()) {
            companionColumnSequences.emplace(column.GetName(), column.GetDefaultFromSequence());
        }
    }
    if (sqlColumnSequences != companionColumnSequences) {
        return Fail(error, "SQL serial columns and companion sequence mappings differ");
    }

    THashSet<TString> sqlSequences;
    for (const auto& sequence : sql.GetSequenceDescription()) {
        if (!sqlSequences.insert(sequence.GetName()).second) {
            return Fail(error, TStringBuilder() << "duplicate SQL sequence: " << sequence.GetName());
        }
    }
    THashSet<TString> companionSequences;
    for (const auto& sequence : companion.GetSequenceDescription()) {
        if (!companionSequences.insert(sequence.GetName()).second) {
            return Fail(error, TStringBuilder() << "duplicate companion sequence: " << sequence.GetName());
        }
    }
    if (sqlSequences != companionSequences) {
        return Fail(error, "SQL and companion table sequence definitions differ");
    }
    return true;
}

bool MakeCompanionRowCreation(
        TSchemeShard* ss,
        const TImportInfo::TItem& item,
        NKikimrSchemeOp::TModifyScheme& result,
        TString& error)
{
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    result.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
    if (!FillTableDescription(result, *item.Table, ss->TableProfiles, status, error, true)) {
        return false;
    }

    Ydb::Table::CreateTableRequest globalIndexes;
    for (const auto& index : item.Table->indexes()) {
        if (!NTableIndex::IsLocalTableIndex(index.type_case())) {
            globalIndexes.add_indexes()->CopyFrom(index);
        }
    }
    if (!FillIndexDescription(
            *result.MutableCreateIndexedTable(),
            globalIndexes,
            ss->EnableCompactFulltextIndex,
            status,
            error))
    {
        return false;
    }


    for (const auto& index : item.Table->indexes()) {
        if (!NTableIndex::IsLocalTableIndex(index.type_case())) {
            continue;
        }

        auto* indexDesc = result.MutableCreateIndexedTable()->AddIndexDescription();
        indexDesc->SetName(index.name());
        indexDesc->MutableKeyColumnNames()->CopyFrom(index.index_columns());
        indexDesc->MutableDataColumnNames()->CopyFrom(index.data_columns());
        indexDesc->SetState(NKikimrSchemeOp::EIndexStateReady);

        switch (index.type_case()) {
        case Ydb::Table::TableIndex::kLocalBloomFilterIndex: {
            indexDesc->SetType(NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
            const auto& source = index.local_bloom_filter_index();
            if (source.has_false_positive_probability()) {
                indexDesc->MutableBloomFilterDescription()->SetFalsePositiveProbability(
                    source.false_positive_probability());
            }
            break;
        }
        case Ydb::Table::TableIndex::kLocalBloomNgramFilterIndex: {
            indexDesc->SetType(NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter);
            const auto& source = index.local_bloom_ngram_filter_index();
            auto* target = indexDesc->MutableBloomNGrammFilterDescription();
            if (source.ngram_size()) {
                target->SetNGrammSize(source.ngram_size());
            }
            if (source.hashes_count()) {
                target->SetHashesCount(source.hashes_count());
            }
            if (source.filter_size_bytes()) {
                target->SetFilterSizeBytes(source.filter_size_bytes());
            }
            if (source.records_count()) {
                target->SetRecordsCount(source.records_count());
            }
            if (source.has_case_sensitive()) {
                target->SetCaseSensitive(source.case_sensitive());
            }
            if (source.has_false_positive_probability()) {
                target->SetFalsePositiveProbability(source.false_positive_probability());
            }
            break;
        }
        case Ydb::Table::TableIndex::kLocalMinMaxIndex:
            indexDesc->SetType(NKikimrSchemeOp::EIndexTypeLocalMinMax);
            break;
        default:
            return Fail(error, TStringBuilder()
                << "unsupported local table index type: " << static_cast<int>(index.type_case()));
        }
    }

    for (const auto& column : item.Table->columns()) {
        if (column.default_value_case() != Ydb::Table::ColumnMeta::kFromSequence) {
            continue;
        }
        auto* sequence = result.MutableCreateIndexedTable()->AddSequenceDescription();
        if (!FillSequenceDescription(*sequence, column.from_sequence(), status, error)) {
            return false;
        }
    }
    return true;
}

bool MakeCompanionColumnCreation(
        const TImportInfo::TItem& item,
        NKikimrSchemeOp::TModifyScheme& result,
        TString& error)
{
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    result.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnTable);
    return FillColumnTableDescription(result, *item.Table, status, error);
}

void MergeRowCompanionSettings(
        NKikimrSchemeOp::TModifyScheme& normalized,
        const NKikimrSchemeOp::TModifyScheme& companion,
        bool buildIndexes)
{
    auto& target = *normalized.MutableCreateIndexedTable();
    const auto& source = companion.GetCreateIndexedTable();
    auto& table = *target.MutableTableDescription();
    const auto& sourceTable = source.GetTableDescription();

    if (!table.HasPartitionConfig() && sourceTable.HasPartitionConfig()) {
        table.MutablePartitionConfig()->CopyFrom(sourceTable.GetPartitionConfig());
    }
    if (!table.HasUniformPartitionsCount() && sourceTable.HasUniformPartitionsCount()) {
        table.SetUniformPartitionsCount(sourceTable.GetUniformPartitionsCount());
    }
    if (table.SplitBoundarySize() == 0) {
        table.MutableSplitBoundary()->CopyFrom(sourceTable.GetSplitBoundary());
    }
    if (!table.HasTTLSettings() && sourceTable.HasTTLSettings()) {
        table.MutableTTLSettings()->CopyFrom(sourceTable.GetTTLSettings());
    }
    if (table.MultiColumnStatisticsSize() == 0) {
        table.MutableMultiColumnStatistics()->CopyFrom(sourceTable.GetMultiColumnStatistics());
    }
    if (!normalized.HasAlterUserAttributes() && companion.HasAlterUserAttributes()) {
        normalized.MutableAlterUserAttributes()->CopyFrom(companion.GetAlterUserAttributes());
    }

    THashMap<TString, const NKikimrSchemeOp::TIndexCreationConfig*> companionIndexes;
    for (const auto& index : source.GetIndexDescription()) {
        companionIndexes[index.GetName()] = &index;
    }

    TVector<NKikimrSchemeOp::TIndexCreationConfig> retained;
    retained.reserve(target.IndexDescriptionSize());
    for (const auto& index : target.GetIndexDescription()) {
        if (buildIndexes && !IsLocalIndex(NTableIndex::GetIndexType(index))) {
            continue;
        }
        retained.push_back(index);
        if (const auto it = companionIndexes.find(index.GetName()); it != companionIndexes.end()
            && retained.back().IndexImplTableDescriptionsSize() == 0)
        {
            retained.back().MutableIndexImplTableDescriptions()->CopyFrom(
                it->second->GetIndexImplTableDescriptions());
        }
    }
    target.ClearIndexDescription();
    for (auto& index : retained) {
        target.AddIndexDescription()->Swap(&index);
    }

    THashMap<TString, size_t> sequences;
    for (size_t i = 0; i < target.SequenceDescriptionSize(); ++i) {
        sequences[target.GetSequenceDescription(i).GetName()] = i;
    }
    for (const auto& sequence : source.GetSequenceDescription()) {
        if (const auto it = sequences.find(sequence.GetName()); it != sequences.end()) {
            target.MutableSequenceDescription(it->second)->MergeFrom(sequence);
        } else {
            target.AddSequenceDescription()->CopyFrom(sequence);
        }
    }
}

void MergeColumnCompanionSettings(
        NKikimrSchemeOp::TModifyScheme& normalized,
        const NKikimrSchemeOp::TModifyScheme& companion)
{
    auto& target = *normalized.MutableCreateColumnTable();
    const auto& source = companion.GetCreateColumnTable();
    if (!target.HasColumnShardCount() && source.HasColumnShardCount()) {
        target.SetColumnShardCount(source.GetColumnShardCount());
    }
    if (!target.HasSharding() && source.HasSharding()) {
        target.MutableSharding()->CopyFrom(source.GetSharding());
    }
    if (!target.HasStorageConfig() && source.HasStorageConfig()) {
        target.MutableStorageConfig()->CopyFrom(source.GetStorageConfig());
    }
    if (target.MultiColumnStatisticsSize() == 0) {
        target.MutableMultiColumnStatistics()->CopyFrom(source.GetMultiColumnStatistics());
    }
    if (!normalized.HasAlterUserAttributes() && companion.HasAlterUserAttributes()) {
        normalized.MutableAlterUserAttributes()->CopyFrom(companion.GetAlterUserAttributes());
    }
}

} // namespace

bool PrepareTableCreationQuery(
        const TString& script,
        const TString& destinationPath,
        TString& creationQuery,
        TString& error)
{
    creationQuery.clear();
    error.clear();

    if (destinationPath.empty()) {
        return Fail(creationQuery, error, "table destination path is empty");
    }

    TRule_sql_query query;
    NYql::TIssues parseIssues;
    if (!NYdb::NDump::SqlToProtoAst(script, query, parseIssues)) {
        return Fail(
            creationQuery,
            error,
            TStringBuilder() << "cannot parse table creation SQL: " << parseIssues.ToOneLineString());
    }

    TVector<const TRule_sql_stmt*> statements;
    FindMessages(query, statements);
    if (statements.size() != 1) {
        return Fail(
            creationQuery,
            error,
            TStringBuilder() << "expected exactly one SQL statement, got " << statements.size());
    }

    TVector<const TRule_create_table_stmt*> createTables;
    FindMessages(query, createTables);
    if (createTables.size() != 1) {
        return Fail(creationQuery, error, "the only SQL statement must be CREATE TABLE");
    }

    const auto& createTable = *createTables.front();
    if (!createTable.GetBlock3().HasAlt1()) {
        return Fail(creationQuery, error, "only a non-temporary row or column CREATE TABLE is supported");
    }

    const auto& tableRefCore = createTable.GetRule_simple_table_ref5().GetRule_simple_table_ref_core1();
    if (tableRefCore.Alt_case() != TRule_simple_table_ref_core::kAltSimpleTableRefCore1) {
        return Fail(creationQuery, error, "CREATE TABLE target must be a static object path");
    }

    TQueryRenderer renderer(&tableRefCore, destinationPath);
    const TString rewritten = renderer.Render(query);

    NYql::TIssues formatIssues;
    if (!NYdb::NDump::Format(rewritten, creationQuery, formatIssues)) {
        return Fail(
            creationQuery,
            error,
            TStringBuilder() << "cannot format rewritten table creation SQL: " << formatIssues.ToOneLineString());
    }

    return true;
}

bool NormalizeTableCreationForImport(
        TSchemeShard* ss,
        const TImportInfo& importInfo,
        ui32 itemIdx,
        NKikimrSchemeOp::TModifyScheme& prepared,
        TString& error)
{
    error.clear();
    if (!ss || itemIdx >= importInfo.Items.size()) {
        return Fail(error, "invalid table import normalization context");
    }
    const auto& item = importInfo.Items[itemIdx];
    if (!item.Table) {
        return Fail(error, "table SQL has no companion table descriptor");
    }

    const bool companionIsColumn = item.Table->store_type() == Ydb::Table::STORE_TYPE_COLUMN;
    const bool preparedIsColumn = prepared.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnTable
        && prepared.HasCreateColumnTable();
    const bool preparedIsRow = IsIn({
        NKikimrSchemeOp::ESchemeOpCreateTable,
        NKikimrSchemeOp::ESchemeOpCreateIndexedTable,
    }, prepared.GetOperationType());
    if (companionIsColumn != preparedIsColumn || (!preparedIsColumn && !preparedIsRow)) {
        return Fail(error, "SQL and companion table storage kinds differ");
    }

    NKikimrSchemeOp::TModifyScheme normalized = prepared;
    NKikimrSchemeOp::TModifyScheme companion;

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    std::pair<TString, TString> wdAndPath;
    if (!TrySplitPathByDb(item.DstPathName, domainPath.PathString(), wdAndPath, error)) {
        return false;
    }
    normalized.SetWorkingDir(wdAndPath.first);
    normalized.SetInternal(true);

    if (preparedIsColumn) {
        if (!MakeCompanionColumnCreation(item, companion, error)) {
            return false;
        }
        auto& table = *normalized.MutableCreateColumnTable();
        const auto& companionTable = companion.GetCreateColumnTable();
        if (!table.HasSchema() || !companionTable.HasSchema()) {
            return Fail(error, "column table schema is missing");
        }
        if (!ValidateColumns(table.GetSchema().GetColumns(), companionTable.GetSchema().GetColumns(), error)) {
            return false;
        }
        if (CopyStrings(table.GetSchema().GetKeyColumnNames())
            != CopyStrings(companionTable.GetSchema().GetKeyColumnNames()))
        {
            return Fail(error, "SQL and companion primary keys differ");
        }
        THashSet<TString> sqlIndexes;
        THashSet<TString> companionIndexes;
        for (const auto& index : table.GetSchema().GetIndexes()) {
            sqlIndexes.insert(index.GetName());
        }
        for (const auto& index : companionTable.GetSchema().GetIndexes()) {
            companionIndexes.insert(index.GetName());
        }
        if (sqlIndexes != companionIndexes) {
            return Fail(error, "SQL and companion column-table index definitions differ");
        }
        table.SetName(wdAndPath.second);
        table.SetIsRestore(true);
        MergeColumnCompanionSettings(normalized, companion);
    } else {
        if (!MakeCompanionRowCreation(ss, item, companion, error)) {
            return false;
        }
        if (normalized.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable) {
            NKikimrSchemeOp::TTableDescription table;
            table.Swap(normalized.MutableCreateTable());
            normalized.ClearCreateTable();
            normalized.MutableCreateIndexedTable()->MutableTableDescription()->Swap(&table);
            normalized.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
        }
        if (normalized.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateIndexedTable
            || !normalized.HasCreateIndexedTable()
            || !normalized.GetCreateIndexedTable().HasTableDescription())
        {
            return Fail(error, "unsupported prepared row-table creation operation");
        }

        auto& table = *normalized.MutableCreateIndexedTable()->MutableTableDescription();
        const auto& companionTable = companion.GetCreateIndexedTable().GetTableDescription();
        if (!ValidateColumns(table.GetColumns(), companionTable.GetColumns(), error)) {
            return false;
        }
        if (CopyStrings(table.GetKeyColumnNames()) != CopyStrings(companionTable.GetKeyColumnNames())) {
            return Fail(error, "SQL and companion primary keys differ");
        }
        if (!ValidateIndexes(normalized.GetCreateIndexedTable(), companion.GetCreateIndexedTable(), error)) {
            return false;
        }
        if (!ValidateSequenceMappings(
                normalized.GetCreateIndexedTable(), companion.GetCreateIndexedTable(), error))
        {
            return false;
        }

        table.SetName(wdAndPath.second);
        table.SetIsRestore(true);
        MergeRowCompanionSettings(normalized, companion, NeedToBuildIndexes(importInfo, itemIdx));
    }

    prepared.Swap(&normalized);
    return true;
}

} // namespace NKikimr::NSchemeShard

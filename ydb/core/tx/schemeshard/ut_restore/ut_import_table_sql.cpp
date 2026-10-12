#include <ydb/core/tx/schemeshard/schemeshard_import_table_scheme.h>
#include <ydb/core/tx/schemeshard/schemeshard_import_flow_proposals.h>
#include <ydb/core/tx/schemeshard/schemeshard_import_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

NKikimrImport::TTableSchemaImportPlan Prepare(const TString& sql) {
    NKikimrImport::TTableSchemaImportPlan plan;
    NYql::TIssues issues;
    UNIT_ASSERT_C(
        PrepareTableImportSql(sql, "/Root/NewDb", "/Root/NewDb/Restored/Table", plan, issues),
        issues.ToString());
    return plan;
}

void AssertRejected(const TString& sql) {
    NKikimrImport::TTableSchemaImportPlan plan;
    NYql::TIssues issues;
    UNIT_ASSERT(!PrepareTableImportSql(
        sql, "/Root/NewDb", "/Root/NewDb/Restored/Table", plan, issues));
    UNIT_ASSERT(!issues.Empty());
    UNIT_ASSERT_VALUES_EQUAL(plan.StatementsSize(), 0);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TImportTableSqlPlanTests) {

    Y_UNIT_TEST(SqlTablePlanRewritesAllObjectTargets) {
        const auto plan = Prepare(R"sql(
            CREATE TABLE `Old/Nested/Table` (
                id Uint64,
                PRIMARY KEY (id)
            );
            ALTER SEQUENCE `Old/Nested/Table/seq` RESTART WITH 42;
            ALTER TABLE `Old/Nested/Table` ADD INDEX `bf` LOCAL USING bloom_filter ON (`id`);
            ALTER OBJECT `Old/Nested/Table` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`='SIMPLE');
            ALTER TABLE `Old/Nested/Table` ALTER INDEX `global` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);
            ALTER TABLE `Old/Nested/Table` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON');
            ALTER TOPIC `Old/Nested/Table/feed/streamImpl` SET (RETENTION_PERIOD = Interval('P1D'));
        )sql");

        UNIT_ASSERT_VALUES_EQUAL(plan.StatementsSize(), 7);
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "CREATE TABLE `Restored/Table`");
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "ALTER SEQUENCE `Restored/Table/seq`");
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "ALTER TOPIC `Restored/Table/feed/streamImpl`");
        UNIT_ASSERT(!plan.GetRewrittenScript().Contains("Old/Nested/Table"));
        UNIT_ASSERT_VALUES_EQUAL(plan.GetStatements(0).GetTarget(), "/Root/NewDb/Restored/Table");
        UNIT_ASSERT_VALUES_EQUAL(plan.GetStatements(1).GetTarget(), "/Root/NewDb/Restored/Table/seq");
        UNIT_ASSERT_VALUES_EQUAL(plan.GetStatements(2).GetIndexName(), "bf");
        UNIT_ASSERT_VALUES_EQUAL(plan.GetStatements(6).GetTarget(), "/Root/NewDb/Restored/Table/feed/streamImpl");
        UNIT_ASSERT(plan.GetStatements(4).GetPopulationDependentIndexSettings());
        UNIT_ASSERT_VALUES_EQUAL(plan.GetStatements(4).GetIndexName(), "global");
    }

    Y_UNIT_TEST(SqlTablePlanPreservesExpressionsAndLiterals) {
        const TString sql = R"sql(
            -- CREATE TABLE `not/a/target`; ALTER TABLE `still/not/a/target`
            CREATE TABLE `Old/Nested/\`Table` (
                `key` Uint64,
                `text` Utf8 DEFAULT 'CREATE TABLE; ALTER TABLE `literal/path`',
                `generated` Utf8 GENERATED ALWAYS AS (
                    String::Concat(`text`, '; ALTER TOPIC `literal/topic`')
                ) STORED,
                PRIMARY KEY (`key`)
            );
            ALTER SEQUENCE `Old/Nested/\`Table/seq` RESTART WITH 7;
        )sql";

        const auto plan = Prepare(sql);
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "-- CREATE TABLE `not/a/target`; ALTER TABLE `still/not/a/target`");
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "'CREATE TABLE; ALTER TABLE `literal/path`'");
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "'; ALTER TOPIC `literal/topic`'");
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "CREATE TABLE `Restored/Table`");
        UNIT_ASSERT_STRING_CONTAINS(plan.GetRewrittenScript(), "ALTER SEQUENCE `Restored/Table/seq`");
    }

    Y_UNIT_TEST(SqlTablePlanAccountsForWholeScript) {
        AssertRejected(R"sql(
            CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));
            ALTER SEQUENCE `Old/Table/seq` RESTART;
            ALTER TABLE `Old/Table` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON');
            SELECT 1;
        )sql");
    }

    Y_UNIT_TEST(SqlTablePlanRejectsUnrelatedMutations) {
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); CREATE TABLE `Old/Other` (id Uint64, PRIMARY KEY (id));");
        AssertRejected("CREATE TABLE IF NOT EXISTS `Old/Table` (id Uint64, PRIMARY KEY (id));");
        AssertRejected("CREATE TEMP TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)) AS VALUES (1);");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER TABLE `Old/Other` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON');");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); UPSERT INTO `Old/Table` (id) VALUES (1);");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER SEQUENCE `Old/Table/../escape` RESTART;");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER OBJECT `Old/Table` (TYPE TABLE) SET (ACTION=DROP_INDEX, NAME=idx);");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER TABLE `Old/Table` ADD INDEX local GLOBAL ON (id);");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER TABLE `Old/Table` ADD INDEX first LOCAL USING bloom_filter ON (id), ADD INDEX second LOCAL USING bloom_filter ON (id);");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER TOPIC `Old/Table/feed/streamImpl` ADD CONSUMER consumer;");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER TOPIC `Old/Table/feed/streamImpl` ALTER CONSUMER consumer SET (IMPORTANT = true);");
        AssertRejected("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id)); ALTER TOPIC `Old/Table/feed/streamImpl` DROP CONSUMER consumer;");
    }

    Y_UNIT_TEST(SqlTablePlanRoundTripsPersistentProto) {
        auto original = Prepare("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));");
        original.SetCurrentOperationAccepted(true);
        original.SetForceBuildIndexes(true);
        NKikimrImport::TTableSchemaImportPlan recovered;
        UNIT_ASSERT(recovered.ParseFromString(original.SerializeAsString()));
        UNIT_ASSERT_VALUES_EQUAL(recovered.GetSource(), NKikimrImport::TTableSchemaImportPlan::SCHEMA_SOURCE_SQL);
        UNIT_ASSERT_VALUES_EQUAL(recovered.GetRewrittenScript(), original.GetRewrittenScript());
        UNIT_ASSERT_VALUES_EQUAL(recovered.StatementsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(recovered.GetStatements(0).GetTarget(), "/Root/NewDb/Restored/Table");
        UNIT_ASSERT(recovered.GetCurrentOperationAccepted());
        UNIT_ASSERT(recovered.GetForceBuildIndexes());
    }

    Y_UNIT_TEST(LegacyImportRowsKeepTheirSource) {
        TImportInfo::TItem item;
        item.Table = Ydb::Table::CreateTableRequest();
        UNIT_ASSERT_VALUES_EQUAL(
            item.GetTableSchemaSource(),
            NKikimrImport::TTableSchemaImportPlan::SCHEMA_SOURCE_PROTOBUF);
        UNIT_ASSERT(!item.UsesSqlSchema());
        UNIT_ASSERT(item.NeedsDataRestore());
    }

    Y_UNIT_TEST(FallbackClearsSqlState) {
        TImportInfo::TItem item;
        item.Table = Ydb::Table::CreateTableRequest();
        item.TableSchemaPlan = Prepare("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));");
        item.CreationQuery = item.TableSchemaPlan->GetRewrittenScript();
        item.PreparedCreationQuery = NKikimrSchemeOp::TModifyScheme();
        item.ResetToProtobufSchema("unsupported generated column");

        UNIT_ASSERT_VALUES_EQUAL(
            item.GetTableSchemaSource(),
            NKikimrImport::TTableSchemaImportPlan::SCHEMA_SOURCE_PROTOBUF);
        UNIT_ASSERT(item.CreationQuery.empty());
        UNIT_ASSERT(!item.PreparedCreationQuery);
        UNIT_ASSERT_VALUES_EQUAL(item.TableSchemaPlan->StatementsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.TableSchemaPlan->GetFallbackReason(), "unsupported generated column");
    }

    Y_UNIT_TEST(SqlTableIgnoresStalePreparationResult) {
        TImportInfo::TItem item;
        item.Table = Ydb::Table::CreateTableRequest();
        item.TableSchemaPlan = Prepare("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));");
        item.TableSchemaPlan->SetPreparationAttempt(7);
        item.ResetToProtobufSchema("fallback");

        UNIT_ASSERT_VALUES_EQUAL(item.TableSchemaPlan->GetPreparationAttempt(), 8);
        UNIT_ASSERT(!item.UsesSqlPreparation());
    }

    Y_UNIT_TEST(SqlFallbackFenceClosesForAcceptedOrPendingProposal) {
        TImportInfo::TItem item;
        item.Table = Ydb::Table::CreateTableRequest();
        item.TableSchemaPlan = Prepare("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));");
        UNIT_ASSERT(item.CanFallbackToProtobuf());

        item.TableSchemaPlan->SetCurrentOperationAccepted(true);
        UNIT_ASSERT(!item.CanFallbackToProtobuf());

        item.TableSchemaPlan->SetCurrentOperationAccepted(false);
        item.WaitTxId = TTxId(42);
        UNIT_ASSERT(!item.CanFallbackToProtobuf());

        item.WaitTxId = InvalidTxId;
        item.TableSchemaPlan->SetFirstMutationCompleted(true);
        UNIT_ASSERT(!item.CanFallbackToProtobuf());
    }

    Y_UNIT_TEST(ProtobufFallbackRestoresOnlyOwnedIndexData) {
        using TSettings = Ydb::Import::ImportFromS3Settings;

        UNIT_ASSERT(!ShouldRestoreIndexImplementationData(
            TSettings::INDEX_POPULATION_MODE_BUILD, false));
        UNIT_ASSERT(!ShouldRestoreIndexImplementationData(
            TSettings::INDEX_POPULATION_MODE_BUILD, true));
        UNIT_ASSERT(ShouldRestoreIndexImplementationData(
            TSettings::INDEX_POPULATION_MODE_IMPORT, false));
        UNIT_ASSERT(ShouldRestoreIndexImplementationData(
            TSettings::INDEX_POPULATION_MODE_IMPORT, true));
        UNIT_ASSERT(ShouldRestoreIndexImplementationData(
            TSettings::INDEX_POPULATION_MODE_AUTO, false));
        UNIT_ASSERT(!ShouldRestoreIndexImplementationData(
            TSettings::INDEX_POPULATION_MODE_AUTO, true));
    }

    Y_UNIT_TEST(CancellationWaitsForPendingSqlProposal) {
        Ydb::Import::ImportFromS3Settings settings;
        TImportInfo importInfo(
            1, "uid", TImportInfo::EKind::S3, settings, TPathId(), "peer");
        auto& item = importInfo.Items.emplace_back("/Root/NewDb/Restored/Table");
        item.Table = Ydb::Table::CreateTableRequest();
        item.TableSchemaPlan = Prepare("CREATE TABLE `Old/Table` (id Uint64, PRIMARY KEY (id));");
        item.State = TImportInfo::EState::CreateSchemeObject;
        item.SubState = TImportInfo::TItem::ESubState::Proposed;

        UNIT_ASSERT(importInfo.HasPendingCancellationWork());
        item.SubState = TImportInfo::TItem::ESubState::AllocateTxId;
        UNIT_ASSERT(!importInfo.HasPendingCancellationWork());
        item.WaitTxId = TTxId(42);
        UNIT_ASSERT(importInfo.HasPendingCancellationWork());
    }

    Y_UNIT_TEST(SqlIndexConversionKeepsUniformPartitioning) {
        NKikimrSchemeOp::TTableDescription table;
        auto* index = table.AddTableIndexes();
        index->SetName("by_value");
        index->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        index->AddKeyColumnNames("value");
        index->AddIndexImplTableDescriptions()->SetUniformPartitionsCount(7);

        Ydb::Table::CreateTableRequest converted;
        FillIndexDescription(converted, table);

        UNIT_ASSERT_VALUES_EQUAL(converted.indexes_size(), 1);
        UNIT_ASSERT(converted.indexes(0).has_global_index());
        UNIT_ASSERT_VALUES_EQUAL(
            converted.indexes(0).global_index().settings().uniform_partitions(), 7);
    }

    Y_UNIT_TEST(ColumnTableDataLayoutPreservesInternalColumnMetadata) {
        NKikimrSchemeOp::TColumnTableDescription columnTable;
        columnTable.SetColumnShardCount(3);
        auto* column = columnTable.MutableSchema()->AddColumns();
        column->SetName("key");
        column->SetType("Decimal(35,10)");
        column->SetTypeId(NScheme::NTypeIds::Decimal);
        column->SetId(17);
        column->SetNotNull(true);
        column->MutableTypeInfo()->SetDecimalPrecision(35);
        column->MutableTypeInfo()->SetDecimalScale(10);
        columnTable.MutableSchema()->AddKeyColumnNames("key");

        const auto layout = MakeColumnTableDataLayout(columnTable);
        UNIT_ASSERT_VALUES_EQUAL(layout.ColumnsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetColumns(0).GetTypeId(), NScheme::NTypeIds::Decimal);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetColumns(0).GetTypeInfo().GetDecimalPrecision(), 35);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetColumns(0).GetTypeInfo().GetDecimalScale(), 10);
        UNIT_ASSERT(layout.GetColumns(0).GetNotNull());
        UNIT_ASSERT_VALUES_EQUAL(layout.GetKeyColumnIds(0), 17);
        UNIT_ASSERT_VALUES_EQUAL(
            layout.GetPartitionConfig().GetPartitioningPolicy().GetMinPartitionsCount(), 3);
    }

    Y_UNIT_TEST(DataLayoutRequiresExactParameterizedType) {
        NKikimrSchemeOp::TColumnTableDescription columnTable;
        auto* destinationColumn = columnTable.MutableSchema()->AddColumns();
        destinationColumn->SetName("key");
        destinationColumn->SetType("Decimal(35,10)");
        destinationColumn->SetTypeId(NScheme::NTypeIds::Decimal);
        destinationColumn->SetId(1);
        destinationColumn->SetNotNull(true);
        destinationColumn->MutableTypeInfo()->SetDecimalPrecision(35);
        destinationColumn->MutableTypeInfo()->SetDecimalScale(10);
        columnTable.MutableSchema()->AddKeyColumnNames("key");

        const auto destination = MakeColumnTableDataLayout(columnTable);
        Ydb::Table::CreateTableRequest backupLayout;
        auto* backupColumn = backupLayout.add_columns();
        backupColumn->set_name("key");
        backupColumn->set_not_null(true);
        backupColumn->mutable_type()->mutable_decimal_type()->set_precision(35);
        backupColumn->mutable_type()->mutable_decimal_type()->set_scale(10);
        backupLayout.add_primary_key("key");

        NKikimrSchemeOp::TTableDescription restoreLayout;
        TString error;
        UNIT_ASSERT_C(
            RebuildTableDescriptionForImport(destination, backupLayout, restoreLayout, error),
            error);
        UNIT_ASSERT_VALUES_EQUAL(
            restoreLayout.GetColumns(0).GetTypeInfo().GetDecimalPrecision(), 35);
        UNIT_ASSERT_VALUES_EQUAL(
            restoreLayout.GetColumns(0).GetTypeInfo().GetDecimalScale(), 10);

        backupColumn->mutable_type()->mutable_decimal_type()->set_scale(9);
        restoreLayout.Clear();
        error.clear();
        UNIT_ASSERT(!RebuildTableDescriptionForImport(
            destination, backupLayout, restoreLayout, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Decimal(35,9)");
        UNIT_ASSERT_STRING_CONTAINS(error, "Decimal(35,10)");

        backupColumn->mutable_type()->set_type_id(Ydb::Type::UTF8);
        restoreLayout.Clear();
        error.clear();
        UNIT_ASSERT(!RebuildTableDescriptionForImport(
            destination, backupLayout, restoreLayout, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Utf8");
    }

    Y_UNIT_TEST(DataLayoutEnforcesGeneratedColumnStorage) {
        NKikimrSchemeOp::TTableDescription destination;
        auto* key = destination.AddColumns();
        key->SetName("key");
        key->SetTypeId(NScheme::NTypeIds::Uint64);
        key->SetId(1);
        destination.AddKeyColumnNames("key");
        destination.AddKeyColumnIds(1);

        auto* stored = destination.AddColumns();
        stored->SetName("stored");
        stored->SetTypeId(NScheme::NTypeIds::Uint64);
        stored->SetId(2);
        stored->MutableDefaultFromExpression()->SetStored(true);

        auto* virtualColumn = destination.AddColumns();
        virtualColumn->SetName("virtual");
        virtualColumn->SetTypeId(NScheme::NTypeIds::Uint64);
        virtualColumn->SetId(3);
        virtualColumn->MutableDefaultFromExpression()->SetStored(false);

        Ydb::Table::CreateTableRequest backupLayout;
        auto* backupKey = backupLayout.add_columns();
        backupKey->set_name("key");
        backupKey->mutable_type()->set_type_id(Ydb::Type::UINT64);
        backupLayout.add_primary_key("key");

        NKikimrSchemeOp::TTableDescription restoreLayout;
        TString error;
        UNIT_ASSERT(!RebuildTableDescriptionForImport(
            destination, backupLayout, restoreLayout, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "missing stored generated column 'stored'");

        auto* backupStored = backupLayout.add_columns();
        backupStored->set_name("stored");
        backupStored->mutable_type()->set_type_id(Ydb::Type::UINT64);
        restoreLayout.Clear();
        error.clear();
        UNIT_ASSERT_C(
            RebuildTableDescriptionForImport(destination, backupLayout, restoreLayout, error),
            error);
        UNIT_ASSERT_VALUES_EQUAL(restoreLayout.ColumnsSize(), 2);
        UNIT_ASSERT(restoreLayout.GetColumns(1).GetDefaultFromExpression().GetStored());

        auto* backupVirtual = backupLayout.add_columns();
        backupVirtual->set_name("virtual");
        backupVirtual->mutable_type()->set_type_id(Ydb::Type::UINT64);
        restoreLayout.Clear();
        error.clear();
        UNIT_ASSERT(!RebuildTableDescriptionForImport(
            destination, backupLayout, restoreLayout, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "virtual generated column 'virtual'");
    }

    Y_UNIT_TEST(PreparedIndexOperationCannotEscapeToNestedDescendant) {
        NKikimrImport::TTableSchemaImportPlan::TStatement statement;
        statement.SetKind(NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_TABLE);
        statement.SetTarget("/Root/NewDb/Restored/Table");
        statement.SetIndexName("index");

        NKikimrSchemeOp::TModifyScheme directChild;
        directChild.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTableIndex);
        directChild.SetWorkingDir(statement.GetTarget());
        directChild.MutableCreateTableIndex()->SetName("index");
        TString error;
        UNIT_ASSERT_C(ValidateSqlTableOperation(directChild, statement, error), error);

        NKikimrSchemeOp::TModifyScheme nestedChild = directChild;
        nestedChild.SetWorkingDir(statement.GetTarget() + "/nested");
        error.clear();
        UNIT_ASSERT(!ValidateSqlTableOperation(nestedChild, statement, error));
        UNIT_ASSERT(!error.empty());

        NKikimrSchemeOp::TModifyScheme sibling = directChild;
        sibling.MutableCreateTableIndex()->SetName("other_index");
        error.clear();
        UNIT_ASSERT(!ValidateSqlTableOperation(sibling, statement, error));
    }

    Y_UNIT_TEST(PreparedIndexSettingsCanOnlyTargetSelectedImplTable) {
        NKikimrImport::TTableSchemaImportPlan::TStatement statement;
        statement.SetKind(NKikimrImport::TTableSchemaImportPlan::STATEMENT_KIND_ALTER_TABLE);
        statement.SetTarget("/Root/NewDb/Restored/Table");
        statement.SetPopulationDependentIndexSettings(true);
        statement.SetIndexName("by_value");

        NKikimrSchemeOp::TModifyScheme operation;
        operation.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        operation.SetWorkingDir(statement.GetTarget() + "/by_value");
        operation.MutableAlterTable()->SetName("indexImplTable");

        TString error;
        UNIT_ASSERT_C(ValidateSqlTableOperation(operation, statement, error), error);

        operation.SetWorkingDir(statement.GetTarget() + "/other_index");
        error.clear();
        UNIT_ASSERT(!ValidateSqlTableOperation(operation, statement, error));

        operation.SetWorkingDir(statement.GetTarget() + "/by_value/nested");
        error.clear();
        UNIT_ASSERT(!ValidateSqlTableOperation(operation, statement, error));

        operation.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTableIndex);
        operation.SetWorkingDir(statement.GetTarget());
        operation.MutableAlterTableIndex()->SetName("other_index");
        error.clear();
        UNIT_ASSERT(!ValidateSqlTableOperation(operation, statement, error));

        operation.MutableAlterTableIndex()->SetName("by_value");
        error.clear();
        UNIT_ASSERT_C(ValidateSqlTableOperation(operation, statement, error), error);
    }
}

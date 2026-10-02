#include <ydb/core/tx/schemeshard/schemeshard_import_table_sql.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSchemeShard {
namespace {

TString Prepare(const TString& script, const TString& destinationPath = "/MyRoot/Nested/Restored") {
    TString creationQuery;
    TString error;
    UNIT_ASSERT_C(
        PrepareTableCreationQuery(script, destinationPath, creationQuery, error),
        error);
    UNIT_ASSERT_C(!creationQuery.empty(), "eligible SQL produced an empty query");
    return creationQuery;
}

void AssertIneligible(const TString& script) {
    TString creationQuery = "must be cleared";
    TString error;
    UNIT_ASSERT(!PrepareTableCreationQuery(script, "/MyRoot/Restored", creationQuery, error));
    UNIT_ASSERT_C(creationQuery.empty(), creationQuery);
    UNIT_ASSERT_C(!error.empty(), "ineligible SQL must explain the fallback reason");
}

} // namespace

Y_UNIT_TEST_SUITE(TImportTableSql) {
    Y_UNIT_TEST(PreparesRowTable) {
        const auto query = Prepare(R"(
            CREATE TABLE source (
                key Uint64 NOT NULL,
                value Utf8,
                PRIMARY KEY (key)
            );
        )");

        UNIT_ASSERT_STRING_CONTAINS(query, "CREATE TABLE");
        UNIT_ASSERT_STRING_CONTAINS(query, "`/MyRoot/Nested/Restored`");
        UNIT_ASSERT_STRING_CONTAINS(query, "PRIMARY KEY");
        UNIT_ASSERT(!query.Contains(" source "));
    }

    Y_UNIT_TEST(PreparesColumnTable) {
        const auto query = Prepare(R"(
            CREATE TABLE source (
                key Uint64 NOT NULL,
                value Utf8,
                PRIMARY KEY (key)
            ) WITH (STORE = COLUMN);
        )");

        UNIT_ASSERT_STRING_CONTAINS(query, "`/MyRoot/Nested/Restored`");
        UNIT_ASSERT_STRING_CONTAINS(query, "STORE = COLUMN");
    }

    Y_UNIT_TEST(RewritesQuotedNameAndNestedDestination) {
        const auto query = Prepare(
            "CREATE TABLE `old/table name` (key Uint64, PRIMARY KEY (key));",
            "/MyRoot/renamed/deep/table name");

        UNIT_ASSERT_STRING_CONTAINS(query, "`/MyRoot/renamed/deep/table name`");
        UNIT_ASSERT(!query.Contains("old/table name"));
    }

    Y_UNIT_TEST(AcceptsCommentsAndSemicolonsInsideLiteral) {
        const auto query = Prepare(R"(
            -- CREATE TABLE decoy (bad Uint64);
            CREATE TABLE source (
                key Uint64,
                value String DEFAULT 'CREATE TABLE decoy; still a literal',
                PRIMARY KEY (key)
            ); -- trailing comment with ;
        )");

        UNIT_ASSERT_STRING_CONTAINS(query, "`/MyRoot/Nested/Restored`");
        UNIT_ASSERT_STRING_CONTAINS(query, "CREATE TABLE decoy; still a literal");
    }

    Y_UNIT_TEST(RejectsEmptyInvalidTemporaryAndOtherObjects) {
        AssertIneligible("");
        AssertIneligible("this is not SQL");
        AssertIneligible("CREATE TEMP TABLE source (key Uint64, PRIMARY KEY (key));");
        AssertIneligible("CREATE VIEW source AS SELECT 1;");
        AssertIneligible("CREATE EXTERNAL TABLE source (key Uint64) WITH (DATA_SOURCE = ds, LOCATION = 'x');");
    }

    Y_UNIT_TEST(RejectsTwoCreateStatementsWithoutExecutablePrefix) {
        AssertIneligible(R"(
            CREATE TABLE first (key Uint64, PRIMARY KEY (key));
            CREATE TABLE second (key Uint64, PRIMARY KEY (key));
        )");
    }

    Y_UNIT_TEST(RejectsChangefeedSuffixWithoutExecutablePrefix) {
        AssertIneligible(R"(
            CREATE TABLE source (key Uint64, PRIMARY KEY (key));
            ALTER TABLE source ADD CHANGEFEED feed
                WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON');
        )");
    }

    Y_UNIT_TEST(RejectsSequenceSuffixWithoutExecutablePrefix) {
        AssertIneligible(R"(
            CREATE TABLE source (key Uint64, PRIMARY KEY (key));
            ALTER SEQUENCE source_seq START WITH 2 INCREMENT BY 3 RESTART WITH 100;
        )");
    }

    Y_UNIT_TEST(RejectsIndexSettingsSuffixWithoutExecutablePrefix) {
        AssertIneligible(R"(
            CREATE TABLE source (
                key Uint64,
                value Utf8,
                INDEX idx GLOBAL ON (value),
                PRIMARY KEY (key)
            );
            ALTER TABLE source ALTER INDEX idx SET (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 64
            );
        )");
    }

    Y_UNIT_TEST(RejectsColumnTableOptionsSuffixWithoutExecutablePrefix) {
        AssertIneligible(R"(
            CREATE TABLE source (key Uint64 NOT NULL, PRIMARY KEY (key))
                WITH (STORE = COLUMN);
            ALTER OBJECT source (TYPE TABLE) SET (
                ACTION = UPSERT_OPTIONS,
                `SCAN_READER_POLICY_NAME` = 'SIMPLE'
            );
        )");
    }
}

} // namespace NKikimr::NSchemeShard

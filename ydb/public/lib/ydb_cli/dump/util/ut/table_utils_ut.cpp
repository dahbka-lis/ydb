#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/table_utils.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NDump {
namespace {

Y_UNIT_TEST_SUITE(TTableUtilsTest) {
    Y_UNIT_TEST(RewritesOnlyTablePathAndPreservesContext) {
        TString query = R"(
            PRAGMA TablePathPrefix = "/OldRoot";
            USE plato;
            CREATE TABLE `/OldRoot/dir/T` (
                k Int32 NOT NULL,
                source_path Utf8,
                v Utf8 GENERATED ALWAYS AS (COALESCE(source_path, '/OldRoot/dir/T')) STORED,
                PRIMARY KEY (k)
            );
        )";
        NYql::TIssues issues;

        UNIT_ASSERT_C(
            RewriteCreateTableQuery(query, "/NewRoot", "/NewRoot/T2", issues),
            issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "`/NewRoot/T2`");
        UNIT_ASSERT_STRING_CONTAINS(query, "PRAGMA TablePathPrefix");
        UNIT_ASSERT_STRING_CONTAINS(query, "USE plato");
        UNIT_ASSERT_STRING_CONTAINS(query, "GENERATED ALWAYS AS");
        UNIT_ASSERT_STRING_CONTAINS(query, "COALESCE");
        UNIT_ASSERT_STRING_CONTAINS(query, "source_path");
        UNIT_ASSERT_STRING_CONTAINS(query, "'/OldRoot/dir/T'");
        UNIT_ASSERT_STRING_CONTAINS(query, "STORED");
    }

    Y_UNIT_TEST(RejectsQueryWithoutCreateTable) {
        TString query = "PRAGMA TablePathPrefix = \"/OldRoot\";";
        const TString original = query;
        NYql::TIssues issues;

        UNIT_ASSERT(!RewriteCreateTableQuery(query, "/NewRoot", "/NewRoot/T2", issues));
        UNIT_ASSERT_VALUES_EQUAL(query, original);
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "expected exactly one CREATE TABLE statement, found 0");
    }

    Y_UNIT_TEST(RejectsAmbiguousCreateTableQuery) {
        TString query = R"(
            CREATE TABLE `/OldRoot/T1` (k Int32 NOT NULL, PRIMARY KEY (k));
            CREATE TABLE `/OldRoot/T2` (k Int32 NOT NULL, PRIMARY KEY (k));
        )";
        const TString original = query;
        NYql::TIssues issues;

        UNIT_ASSERT(!RewriteCreateTableQuery(query, "/NewRoot", "/NewRoot/T3", issues));
        UNIT_ASSERT_VALUES_EQUAL(query, original);
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "expected exactly one CREATE TABLE statement, found 2");
    }

    Y_UNIT_TEST(DispatchIgnoresCreateMarkerInsideGeneratedExpression) {
        TString query = R"(
            PRAGMA TablePathPrefix = "/OldRoot";
            USE plato;
            CREATE TABLE `/OldRoot/T` (
                k Int32 NOT NULL,
                marker Utf8 GENERATED ALWAYS AS ('CREATE VIEW IF NOT EXISTS `/OldRoot/View`') STORED,
                PRIMARY KEY (k)
            );
        )";
        NYql::TIssues issues;

        UNIT_ASSERT_C(
            RewriteSchemeCreateQuery(query, "/NewRoot", "/NewRoot/T2", issues),
            issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "`/NewRoot/T2`");
        UNIT_ASSERT_STRING_CONTAINS(query, "'CREATE VIEW IF NOT EXISTS `/OldRoot/View`'");
        UNIT_ASSERT_STRING_CONTAINS(query, "PRAGMA TablePathPrefix");
        UNIT_ASSERT_STRING_CONTAINS(query, "USE plato");
    }

    Y_UNIT_TEST(SchemeCreatePathIgnoresLeadingSameTypeMarker) {
        TString query = R"(
            $marker = 'CREATE VIEW IF NOT EXISTS `/OldRoot/Decoy`';
            CREATE VIEW IF NOT EXISTS `/OldRoot/View`
                WITH (security_invoker = TRUE)
                AS SELECT $marker;
        )";
        NYql::TIssues issues;

        UNIT_ASSERT_C(
            RewriteSchemeCreateQuery(query, "/NewRoot", "/NewRoot/View", issues),
            issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "`/NewRoot/View`");
        UNIT_ASSERT_STRING_CONTAINS(query, "'CREATE VIEW IF NOT EXISTS `/OldRoot/Decoy`'");
        UNIT_ASSERT(!query.Contains("`/OldRoot/View`"));
    }

    Y_UNIT_TEST(RejectsClusterQualifiedTablePathWithoutMutation) {
        TString query = R"(
            $cluster = "plato";
            CREATE TABLE $cluster.`/OldRoot/T` (
                k Int32 NOT NULL,
                PRIMARY KEY (k)
            );
        )";
        const TString original = query;
        NYql::TIssues issues;

        UNIT_ASSERT(!RewriteCreateTableQuery(query, "/NewRoot", "/NewRoot/T2", issues));
        UNIT_ASSERT_VALUES_EQUAL(query, original);
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "cluster-qualified");
    }
}

} // anonymous
} // NYdb::NDump

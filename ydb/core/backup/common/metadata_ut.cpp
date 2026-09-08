#include "metadata.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(PathsNormalizationTest) {
    Y_UNIT_TEST(NormalizeItemPath) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("/a/b/c/"), "a/b/c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("/"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath(""), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("//"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("///a///b///"), "a/b");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("a/b/c"), "a/b/c");
    }

    Y_UNIT_TEST(NormalizeItemPrefix) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("///a///b///c///"), "a///b///c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("a///b///c"), "a///b///c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("//"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("/"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix(""), "");
    }

    Y_UNIT_TEST(NormalizeExportPrefix) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("///a///"), "///a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("a/"), "a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("a"), "a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("/prefix//"), "/prefix");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix(""), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("/prefix"), "/prefix");
    }
}

Y_UNIT_TEST_SUITE(MetadataTest) {
    Y_UNIT_TEST(TableUserAttributesRoundTrip) {
        TMetadata metadata;
        metadata.SetVersion(1);
        metadata.SetTableUserAttributes({
            {.Key = "backup_test_attribute", .Value = "preserved"},
            {.Key = "empty_attribute", .Value = ""},
        });

        const auto restored = TMetadata::Deserialize(metadata.Serialize());
        const auto& attributes = restored.GetTableUserAttributes();
        UNIT_ASSERT(attributes);
        UNIT_ASSERT_VALUES_EQUAL(attributes->size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(attributes->at(0).Key, "backup_test_attribute");
        UNIT_ASSERT_VALUES_EQUAL(attributes->at(0).Value, "preserved");
        UNIT_ASSERT_VALUES_EQUAL(attributes->at(1).Key, "empty_attribute");
        UNIT_ASSERT_VALUES_EQUAL(attributes->at(1).Value, "");
    }

    Y_UNIT_TEST(LegacyMetadataHasNoTableUserAttributes) {
        const auto metadata = TMetadata::Deserialize(R"({"version": 0})");
        UNIT_ASSERT(!metadata.GetTableUserAttributes());
    }
}

} // namespace NKikimr::NBackup

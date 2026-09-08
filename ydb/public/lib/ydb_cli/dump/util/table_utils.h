#pragma once

#include <util/generic/string.h>

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

bool RewriteCreateTableQuery(
    TString& query,
    const TString& restoreRoot,
    const TString& dstPath,
    NYql::TIssues& issues);

} // NYdb::NDump

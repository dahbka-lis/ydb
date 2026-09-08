#pragma once

#include <util/generic/fwd.h>

namespace NKikimrScheme {
    class TEvDescribeSchemeResult;
}

namespace NKikimrSchemeOp {
    class TPathDescription;
}

namespace NKikimr::NSchemeShard {

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& databaseRoot,
    TString& error);

bool BuildCreateTableQuery(
    const TString& tablePath,
    const NKikimrSchemeOp::TPathDescription& pathDescription,
    TString& query,
    TString& error);

} // namespace NKikimr::NSchemeShard

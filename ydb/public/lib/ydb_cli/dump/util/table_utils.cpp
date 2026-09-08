#include "query_utils.h"
#include "table_utils.h"

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <functional>

namespace NYdb::NDump {

using namespace NSQLv1Generated;

namespace {

template <typename TCallback>
void VisitMutableMessages(NProtoBuf::Message& message, TCallback& callback) {
    callback(message);

    const auto* descriptor = message.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        NProtoBuf::TMutableField field(message, descriptor->field(i));
        if (!field.IsMessage()) {
            continue;
        }

        for (size_t j = 0; j < field.Size(); ++j) {
            VisitMutableMessages(*field.MutableMessage(j), callback);
        }
    }
}

TVector<TRule_create_table_stmt*> FindCreateTableStatements(TRule_sql_query& query) {
    TVector<TRule_create_table_stmt*> statements;
    auto collect = [&statements](NProtoBuf::Message& message) {
        auto* statement = dynamic_cast<TRule_create_table_stmt*>(&message);
        if (statement && statement->GetBlock3().HasAlt1()) {
            statements.push_back(statement);
        }
    };
    VisitMutableMessages(query, collect);
    return statements;
}

TVector<TToken*> FindTokens(NProtoBuf::Message& message) {
    TVector<TToken*> tokens;
    auto collect = [&tokens](NProtoBuf::Message& current) {
        if (auto* token = dynamic_cast<TToken*>(&current)) {
            tokens.push_back(token);
        }
    };
    VisitMutableMessages(message, collect);
    return tokens;
}

TString RenderTokens(const NProtoBuf::Message& message) {
    TStringBuilder result;
    std::function<void(const NProtoBuf::Message&)> visit = [&](const NProtoBuf::Message& current) {
        if (const auto* token = dynamic_cast<const TToken*>(&current)) {
            if (token->GetValue() != "<EOF>") {
                if (!result.empty()) {
                    result << ' ';
                }
                result << token->GetValue();
            }
            return;
        }

        const auto* descriptor = current.GetDescriptor();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            NProtoBuf::TConstField field(current, descriptor->field(i));
            if (!field.IsMessage()) {
                continue;
            }

            for (size_t j = 0; j < field.Size(); ++j) {
                visit(*field.Get<const NProtoBuf::Message*>(j));
            }
        }
    };
    visit(message);
    return result;
}

bool RewriteTablePath(TRule_create_table_stmt& statement, const TString& dstPath, NYql::TIssues& issues) {
    auto* tableRefCore = statement.MutableRule_simple_table_ref5()->MutableRule_simple_table_ref_core1();
    if (!tableRefCore->HasAlt_simple_table_ref_core1()) {
        issues.AddIssue("CREATE TABLE object path must be a literal identifier");
        return false;
    }

    auto* objectRef = tableRefCore->MutableAlt_simple_table_ref_core1()->MutableRule_object_ref1();
    if (objectRef->HasBlock1()) {
        issues.AddIssue("CREATE TABLE object path must not be cluster-qualified");
        return false;
    }

    auto* path = objectRef->MutableRule_id_or_at2();
    if (path->HasBlock1()) {
        issues.AddIssue("CREATE TABLE object path must not use an anonymous table marker");
        return false;
    }

    auto tokens = FindTokens(*path);
    if (tokens.size() != 1) {
        issues.AddIssue(TStringBuilder()
            << "expected one identifier token in CREATE TABLE object path, found " << tokens.size());
        return false;
    }

    tokens.front()->SetValue(TStringBuilder() << '`' << dstPath << '`');
    return true;
}

} // anonymous

bool RewriteCreateTableQuery(
    TString& query,
    const TString& restoreRoot,
    const TString& dstPath,
    NYql::TIssues& issues)
{
    // The destination object path is supplied by the import item and is authoritative.
    // Unlike view bodies, CREATE TABLE expressions cannot reference tables, so there is
    // no path relative to restoreRoot to rewrite.
    Y_UNUSED(restoreRoot);

    TRule_sql_query queryProto;
    if (!SqlToProtoAst(query, queryProto, issues)) {
        return false;
    }

    auto statements = FindCreateTableStatements(queryProto);
    if (statements.size() != 1) {
        issues.AddIssue(TStringBuilder()
            << "expected exactly one CREATE TABLE statement, found " << statements.size());
        return false;
    }

    if (!RewriteTablePath(*statements.front(), dstPath, issues)) {
        return false;
    }

    TString formattedQuery;
    if (!Format(RenderTokens(queryProto), formattedQuery, issues)) {
        return false;
    }

    query = std::move(formattedQuery);
    return true;
}

} // NYdb::NDump

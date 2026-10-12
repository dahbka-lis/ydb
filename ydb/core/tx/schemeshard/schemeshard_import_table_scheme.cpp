#include "schemeshard_import_table_scheme.h"

#include "schemeshard_import_helpers.h"
#include "schemeshard_info_types.h"

#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/folder/pathsplit.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {
namespace {

using namespace NSQLv1Generated;

using TPlan = NKikimrImport::TTableSchemaImportPlan;
constexpr ui32 TableSchemaPlanFormatVersion = 1;

struct TPath {
    bool Absolute = false;
    TVector<TString> Components;
};

struct TReplacement {
    size_t Begin = 0;
    size_t End = 0;
    TString Value;
};

struct TClassifiedStatement {
    TPlan::EStatementKind Kind = TPlan::STATEMENT_KIND_UNSPECIFIED;
    TPlan::EPhase Phase = TPlan::PHASE_BEFORE_DATA;
    const NProtoBuf::Message* Target = nullptr;
    ui32 TargetSuffixComponents = 0;
    bool PopulationDependentIndexSettings = false;
    const NProtoBuf::Message* IndexName = nullptr;
};

bool Fail(NYql::TIssues& issues, TString message) {
    issues.AddIssue(std::move(message));
    return false;
}

void CollectTokens(const NProtoBuf::Message& message, TVector<const TToken*>& tokens) {
    if (const auto* token = dynamic_cast<const TToken*>(&message)) {
        if (token->GetValue() != "<EOF>") {
            tokens.push_back(token);
        }
        return;
    }

    const auto* descriptor = message.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* fieldDescriptor = descriptor->field(i);
        NProtoBuf::TConstField field(message, fieldDescriptor);
        if (!field.IsMessage()) {
            continue;
        }
        for (size_t j = 0; j < field.Size(); ++j) {
            CollectTokens(*field.Get<NProtoBuf::Message>(j), tokens);
        }
    }
}

bool ContainsToken(const NProtoBuf::Message& message, TStringBuf expected) {
    TVector<const TToken*> tokens;
    CollectTokens(message, tokens);
    for (const auto* token : tokens) {
        const auto& value = token->GetValue();
        if (value.size() != expected.size()) {
            continue;
        }
        bool equal = true;
        for (size_t i = 0; i < value.size(); ++i) {
            if (AsciiToUpper(value[i]) != AsciiToUpper(expected[i])) {
                equal = false;
                break;
            }
        }
        if (equal) {
            return true;
        }
    }
    return false;
}

bool IsAllowedAlterObjectAction(const NProtoBuf::Message& message, NYql::TIssues& issues) {
    TVector<const TToken*> tokens;
    CollectTokens(message, tokens);

    ui32 actionCount = 0;
    for (size_t index = 0; index < tokens.size(); ++index) {
        if (!ContainsToken(*tokens[index], "ACTION")) {
            continue;
        }

        ++actionCount;
        if (index + 2 >= tokens.size() || tokens[index + 1]->GetValue() != "=") {
            return Fail(issues, "ALTER OBJECT ACTION has an invalid shape");
        }

        const auto& action = tokens[index + 2]->GetValue();
        if (!ContainsToken(*tokens[index + 2], "ALTER_COLUMN")
            && !ContainsToken(*tokens[index + 2], "UPSERT_INDEX")
            && !ContainsToken(*tokens[index + 2], "UPSERT_OPTIONS")) {
            return Fail(issues, TStringBuilder()
                << "unsupported ALTER OBJECT ACTION in table backup SQL: " << action);
        }
    }

    if (actionCount != 1) {
        return Fail(issues, "table backup ALTER OBJECT must contain exactly one ACTION");
    }
    return true;
}

bool IsAllowedAlterTopicAction(
    const TRule_alter_topic_action& action,
    NYql::TIssues& issues)
{
    switch (action.Alt_case()) {
        case TRule_alter_topic_action::kAltAlterTopicAction4: // SET topic settings
        case TRule_alter_topic_action::kAltAlterTopicAction5: // RESET topic settings
            return true;
        case TRule_alter_topic_action::kAltAlterTopicAction1: // ADD CONSUMER
        case TRule_alter_topic_action::kAltAlterTopicAction2: // ALTER CONSUMER
        case TRule_alter_topic_action::kAltAlterTopicAction3: // DROP CONSUMER
            return Fail(issues,
                "ALTER TOPIC consumer actions are not allowed in table backup SQL; "
                "consumers are restored from CDC sidecars");
        case TRule_alter_topic_action::ALT_NOT_SET:
            return Fail(issues, "ALTER TOPIC action is missing in table backup SQL");
    }
    return Fail(issues, "unsupported ALTER TOPIC action in table backup SQL");
}

bool DecodeIdentifier(TStringBuf token, TString& value, NYql::TIssues& issues) {
    if (token.empty()) {
        return Fail(issues, "empty object reference");
    }

    const char quote = token.front();
    if (quote != '`' && quote != '\'' && quote != '"') {
        value = token;
        return true;
    }
    if (token.size() < 2 || token.back() != quote) {
        return Fail(issues, TStringBuilder() << "invalid quoted object reference: " << token);
    }

    value.clear();
    TStringOutput output(value);
    size_t readBytes = 0;
    const auto result = NYql::UnescapeArbitraryAtom(token.SubStr(1), quote, &output, &readBytes);
    if (result != NYql::EUnescapeResult::OK || readBytes != token.size() - 1) {
        return Fail(issues, TStringBuilder() << "cannot decode object reference: " << token);
    }
    return true;
}

TString EncodeIdentifier(TStringBuf value) {
    TString result;
    TStringOutput output(result);
    NYql::EscapeArbitraryAtom(value, '`', &output);
    return result;
}

bool ParsePath(TStringBuf value, TPath& path, NYql::TIssues& issues) {
    if (value.empty()) {
        return Fail(issues, "empty object path");
    }

    TPathSplitUnix split(value);
    path.Absolute = split.IsAbsolute;
    path.Components.assign(split.begin(), split.end());
    if (path.Components.empty()) {
        return Fail(issues, TStringBuilder() << "object path has no components: " << value);
    }
    for (const auto& component : path.Components) {
        if (component.empty() || component == "." || component == "..") {
            return Fail(issues, TStringBuilder() << "object path is not confined: " << value);
        }
    }
    return true;
}

bool IsPrefix(const TPath& prefix, const TPath& path) {
    if (prefix.Absolute != path.Absolute || prefix.Components.size() > path.Components.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.Components.size(); ++i) {
        if (prefix.Components[i] != path.Components[i]) {
            return false;
        }
    }
    return true;
}

TString JoinPathComponents(const TVector<TString>& components, size_t begin, bool absolute) {
    TStringBuilder result;
    if (absolute) {
        result << '/';
    }
    for (size_t i = begin; i < components.size(); ++i) {
        if (i != begin) {
            result << '/';
        }
        result << components[i];
    }
    return result;
}

bool BuildDestinationPaths(
    const TString& destinationDatabase,
    const TString& destinationPath,
    TPath& database,
    TPath& destination,
    NYql::TIssues& issues)
{
    if (!ParsePath(destinationDatabase, database, issues) || !ParsePath(destinationPath, destination, issues)) {
        return false;
    }
    if (!database.Absolute || !destination.Absolute || !IsPrefix(database, destination)
        || database.Components.size() == destination.Components.size()) {
        return Fail(issues, TStringBuilder()
            << "destination path " << destinationPath.Quote()
            << " must be a child of database " << destinationDatabase.Quote());
    }

    return true;
}

bool SplitDestination(const TString& destination, TString& workingDir, TString& name, TString& error) {
    const size_t slash = destination.rfind('/');
    if (destination.empty() || destination.front() != '/' || slash == TString::npos
        || slash + 1 == destination.size()) {
        error = TStringBuilder() << "invalid import destination path: " << destination;
        return false;
    }
    workingDir = slash ? destination.substr(0, slash) : "/";
    name = destination.substr(slash + 1);
    return true;
}

TString OperationTarget(const NKikimrSchemeOp::TModifyScheme& operation) {
    TString name;
    switch (operation.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateTable:
            name = operation.GetCreateTable().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
            name = operation.GetCreateIndexedTable().GetTableDescription().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
            name = operation.GetCreateColumnTable().GetName();
            break;
        default:
            return {};
    }
    return TStringBuilder() << operation.GetWorkingDir()
        << (operation.GetWorkingDir().EndsWith('/') ? "" : "/") << name;
}

TString PreparedOperationTarget(const NKikimrSchemeOp::TModifyScheme& operation) {
    TString name;
    switch (operation.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateTable:
            name = operation.GetCreateTable().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
            name = operation.GetCreateIndexedTable().GetTableDescription().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
            name = operation.GetCreateColumnTable().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpAlterSequence:
            name = operation.GetSequence().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpAlterTable:
            name = operation.GetAlterTable().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpAlterColumnTable:
            name = operation.GetAlterColumnTable().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpCreateTableIndex:
            name = operation.GetCreateTableIndex().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpAlterTableIndex:
            name = operation.GetAlterTableIndex().GetName();
            break;
        case NKikimrSchemeOp::ESchemeOpCreateCdcStream:
            name = operation.GetCreateCdcStream().GetTableName();
            break;
        case NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup:
            name = operation.GetAlterPersQueueGroup().GetName();
            break;
        default:
            return {};
    }
    return TStringBuilder() << operation.GetWorkingDir()
        << (operation.GetWorkingDir().EndsWith('/') ? "" : "/") << name;
}

bool TokenOffset(const TString& sql, const TToken& token, size_t& offset, NYql::TIssues& issues) {
    if (!token.GetLine()) {
        return Fail(issues, "SQL parser returned a token without a line number");
    }

    size_t line = 1;
    size_t lineStart = 0;
    while (line < token.GetLine()) {
        const size_t newline = sql.find('\n', lineStart);
        if (newline == TString::npos) {
            return Fail(issues, "SQL parser returned an out-of-range token line");
        }
        lineStart = newline + 1;
        ++line;
    }

    offset = lineStart + token.GetColumn();
    const auto& value = token.GetValue();
    if (offset > sql.size() || value.size() > sql.size() - offset
        || TStringBuf(sql).SubStr(offset, value.size()) != value) {
        return Fail(issues, TStringBuilder()
            << "SQL parser token position does not match source text at "
            << token.GetLine() << ':' << token.GetColumn());
    }
    return true;
}

bool ExtractTarget(
    const TString& sql,
    const NProtoBuf::Message& target,
    TString& decodedTarget,
    size_t& begin,
    size_t& end,
    NYql::TIssues& issues)
{
    TVector<const TToken*> tokens;
    CollectTokens(target, tokens);
    if (tokens.size() != 1) {
        return Fail(issues, "table backup SQL requires a static, unqualified object path");
    }

    if (!TokenOffset(sql, *tokens.front(), begin, issues)) {
        return false;
    }
    end = begin + tokens.front()->GetValue().size();
    return DecodeIdentifier(tokens.front()->GetValue(), decodedTarget, issues);
}

bool ExtractIdentifier(
    const NProtoBuf::Message& identifier,
    TString& decoded,
    NYql::TIssues& issues)
{
    TVector<const TToken*> tokens;
    CollectTokens(identifier, tokens);
    if (tokens.size() != 1) {
        return Fail(issues, "table backup SQL requires a static index name");
    }
    return DecodeIdentifier(tokens.front()->GetValue(), decoded, issues);
}

bool ClassifyAlterTableAction(
    const TRule_alter_table_action& action,
    TPlan::EPhase& phase,
    bool& populationDependentIndexSettings,
    const NProtoBuf::Message*& indexName,
    NYql::TIssues& issues)
{
    populationDependentIndexSettings = false;
    indexName = nullptr;
    switch (action.Alt_case()) {
        case TRule_alter_table_action::kAltAlterTableAction9: { // ADD INDEX
            const auto& tableIndex = action.GetAlt_alter_table_action9()
                .GetRule_alter_table_add_index1()
                .GetRule_table_index2();
            if (tableIndex
                    .GetRule_table_index_type3()
                    .GetBlock1()
                    .Alt_case() != TRule_table_index_type_TBlock1::kAlt2) {
                return Fail(issues, "only local ADD INDEX is allowed in table backup SQL");
            }
            phase = TPlan::PHASE_BEFORE_DATA;
            indexName = &tableIndex.GetRule_an_id2();
            return true;
        }
        case TRule_alter_table_action::kAltAlterTableAction12: // ADD CHANGEFEED
            phase = TPlan::PHASE_CHANGEFEEDS;
            return true;
        case TRule_alter_table_action::kAltAlterTableAction16: // ALTER INDEX
            phase = TPlan::PHASE_AFTER_INDEXES;
            populationDependentIndexSettings = true;
            indexName = &action.GetAlt_alter_table_action16()
                .GetRule_alter_table_alter_index1().GetRule_an_id3();
            return true;
        default:
            return Fail(issues, "unsupported ALTER TABLE action in table backup SQL");
    }
}

bool ClassifyStatement(
    const TRule_sql_stmt_core& statement,
    ui32 ordinal,
    TClassifiedStatement& result,
    NYql::TIssues& issues)
{
    switch (statement.Alt_case()) {
        case TRule_sql_stmt_core::kAltSqlStmtCore4: { // CREATE TABLE
            const auto& create = statement.GetAlt_sql_stmt_core4().GetRule_create_table_stmt1();
            if (ordinal != 0) {
                return Fail(issues, "CREATE TABLE must be the first statement in table backup SQL");
            }
            if (create.HasBlock2()) {
                return Fail(issues, "CREATE OR REPLACE is not allowed in table backup SQL");
            }
            if (!create.GetBlock3().HasAlt1()) {
                return Fail(issues, "only a persistent CREATE TABLE is allowed in table backup SQL");
            }
            if (create.HasBlock4()) {
                return Fail(issues, "CREATE TABLE IF NOT EXISTS is not allowed in table backup SQL");
            }
            if (create.HasBlock15()) {
                return Fail(issues, "CREATE TABLE AS is not allowed in table backup SQL");
            }
            result.Kind = TPlan::STATEMENT_KIND_CREATE_TABLE;
            result.Phase = TPlan::PHASE_BEFORE_DATA;
            result.Target = &create.GetRule_simple_table_ref5();
            return true;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore15: { // ALTER TABLE
            const auto& alter = statement.GetAlt_sql_stmt_core15().GetRule_alter_table_stmt1();
            TPlan::EPhase phase;
            bool populationDependentIndexSettings = false;
            const NProtoBuf::Message* indexName = nullptr;
            if (!ClassifyAlterTableAction(
                    alter.GetRule_alter_table_action4(), phase,
                    populationDependentIndexSettings, indexName, issues)) {
                return false;
            }
            for (const auto& block : alter.GetBlock5()) {
                TPlan::EPhase nextPhase;
                bool nextPopulationDependentIndexSettings = false;
                const NProtoBuf::Message* nextIndexName = nullptr;
                if (!ClassifyAlterTableAction(
                        block.GetRule_alter_table_action2(), nextPhase,
                        nextPopulationDependentIndexSettings, nextIndexName, issues)) {
                    return false;
                }
                if (nextPhase != phase) {
                    return Fail(issues, "ALTER TABLE actions from different import phases must be separate statements");
                }
                if (populationDependentIndexSettings || nextPopulationDependentIndexSettings) {
                    return Fail(issues, "ALTER INDEX cannot be combined with another ALTER TABLE action");
                }
                if (indexName || nextIndexName) {
                    return Fail(issues,
                        "multiple ADD INDEX actions must use separate ALTER TABLE statements");
                }
            }
            result.Kind = TPlan::STATEMENT_KIND_ALTER_TABLE;
            result.Phase = phase;
            result.Target = &alter.GetRule_simple_table_ref3();
            result.PopulationDependentIndexSettings = populationDependentIndexSettings;
            result.IndexName = indexName;
            return true;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore28: { // ALTER OBJECT
            const auto& alter = statement.GetAlt_sql_stmt_core28().GetRule_alter_object_stmt1();
            if (!ContainsToken(alter.GetRule_object_type_ref6(), "TABLE")) {
                return Fail(issues, "only ALTER OBJECT TYPE TABLE is allowed in table backup SQL");
            }
            if (!IsAllowedAlterObjectAction(alter, issues)) {
                return false;
            }
            result.Kind = TPlan::STATEMENT_KIND_ALTER_OBJECT;
            result.Phase = TPlan::PHASE_BEFORE_DATA;
            result.Target = &alter.GetRule_object_ref3();
            return true;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore36: { // ALTER TOPIC
            const auto& alter = statement.GetAlt_sql_stmt_core36().GetRule_alter_topic_stmt1();
            if (alter.HasBlock3()) {
                return Fail(issues, "ALTER TOPIC IF EXISTS is not allowed in table backup SQL");
            }
            if (!IsAllowedAlterTopicAction(alter.GetRule_alter_topic_action5(), issues)) {
                return false;
            }
            for (const auto& block : alter.GetBlock6()) {
                if (!IsAllowedAlterTopicAction(block.GetRule_alter_topic_action2(), issues)) {
                    return false;
                }
            }
            result.Kind = TPlan::STATEMENT_KIND_ALTER_TOPIC;
            result.Phase = TPlan::PHASE_CHANGEFEEDS;
            result.Target = &alter.GetRule_topic_ref4();
            result.TargetSuffixComponents = 2; // <table>/<stream>/streamImpl
            return true;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore57: { // ALTER SEQUENCE
            const auto& alter = statement.GetAlt_sql_stmt_core57().GetRule_alter_sequence_stmt1();
            if (alter.HasBlock3()) {
                return Fail(issues, "ALTER SEQUENCE IF EXISTS is not allowed in table backup SQL");
            }
            result.Kind = TPlan::STATEMENT_KIND_ALTER_SEQUENCE;
            result.Phase = TPlan::PHASE_BEFORE_DATA;
            result.Target = &alter.GetRule_object_ref4();
            result.TargetSuffixComponents = 1;
            return true;
        }
        default:
            return Fail(issues, TStringBuilder()
                << "unsupported statement " << ordinal << " in table backup SQL");
    }
}

bool SplitStatements(const TString& sql, TVector<TString>& statements, NYql::TIssues& issues) {
    NSQLTranslation::TTranslationSettings settings;
    if (!NSQLTranslation::ParseTranslationSettings(sql, settings, issues)) {
        return false;
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    return NSQLTranslationV1::SplitQueryToStatements(
        sql,
        NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer),
        statements,
        issues);
}

} // anonymous namespace

bool PrepareTableImportSql(
    const TString& sql,
    const TString& destinationDatabase,
    const TString& destinationPath,
    TPlan& plan,
    NYql::TIssues& issues)
{
    plan.Clear();
    if (sql.empty()) {
        return Fail(issues, "table backup SQL is empty");
    }

    TRule_sql_query query;
    if (!NYdb::NDump::SqlToProtoAst(sql, query, issues)) {
        return false;
    }
    const auto statements = NSQLTranslationV1::Statements(query);
    if (statements.empty()) {
        return Fail(issues, "table backup SQL contains no statements");
    }

    TPath database;
    TPath destination;
    if (!BuildDestinationPaths(destinationDatabase, destinationPath, database, destination, issues)) {
        return false;
    }

    TVector<TClassifiedStatement> classified;
    classified.reserve(statements.size());
    for (ui32 ordinal = 0; ordinal < statements.size(); ++ordinal) {
        TClassifiedStatement current;
        if (!ClassifyStatement(*statements[ordinal], ordinal, current, issues)) {
            return false;
        }
        classified.push_back(current);
    }
    if (classified.front().Kind != TPlan::STATEMENT_KIND_CREATE_TABLE) {
        return Fail(issues, "table backup SQL must contain exactly one root CREATE TABLE");
    }
    if (CountIf(classified, [](const auto& item) {
            return item.Kind == TPlan::STATEMENT_KIND_CREATE_TABLE;
        }) != 1) {
        return Fail(issues, "table backup SQL must contain exactly one root CREATE TABLE");
    }

    TString sourceRootText;
    size_t unusedBegin = 0;
    size_t unusedEnd = 0;
    if (!ExtractTarget(sql, *classified.front().Target, sourceRootText, unusedBegin, unusedEnd, issues)) {
        return false;
    }
    TPath sourceRoot;
    if (!ParsePath(sourceRootText, sourceRoot, issues)) {
        return false;
    }

    TVector<TReplacement> replacements;
    replacements.reserve(statements.size());
    TVector<TString> absoluteTargets;
    absoluteTargets.reserve(statements.size());
    TVector<TString> indexNames(statements.size());
    for (ui32 ordinal = 0; ordinal < statements.size(); ++ordinal) {
        TString sourceTargetText;
        size_t begin = 0;
        size_t end = 0;
        if (!ExtractTarget(sql, *classified[ordinal].Target, sourceTargetText, begin, end, issues)) {
            return false;
        }

        TPath sourceTarget;
        if (!ParsePath(sourceTargetText, sourceTarget, issues)) {
            return false;
        }
        if (!IsPrefix(sourceRoot, sourceTarget)) {
            return Fail(issues, TStringBuilder()
                << "statement " << ordinal << " targets an object outside the root table: "
                << sourceTargetText);
        }

        const size_t suffixSize = sourceTarget.Components.size() - sourceRoot.Components.size();
        if (suffixSize != classified[ordinal].TargetSuffixComponents) {
            return Fail(issues, TStringBuilder()
                << "statement " << ordinal << " has an invalid target for its kind: "
                << sourceTargetText);
        }

        TVector<TString> destinationTargetComponents = destination.Components;
        destinationTargetComponents.insert(
            destinationTargetComponents.end(),
            sourceTarget.Components.begin() + sourceRoot.Components.size(),
            sourceTarget.Components.end());
        const TString absoluteTarget = JoinPathComponents(destinationTargetComponents, 0, true);
        const TString sqlTarget = JoinPathComponents(destinationTargetComponents, database.Components.size(), false);
        replacements.push_back({begin, end, EncodeIdentifier(sqlTarget)});
        absoluteTargets.push_back(absoluteTarget);
        if (classified[ordinal].IndexName
            && !ExtractIdentifier(*classified[ordinal].IndexName, indexNames[ordinal], issues)) {
            return false;
        }
    }

    Sort(replacements, [](const auto& lhs, const auto& rhs) {
        return lhs.Begin > rhs.Begin;
    });
    TString rewritten = sql;
    for (const auto& replacement : replacements) {
        rewritten.replace(replacement.Begin, replacement.End - replacement.Begin, replacement.Value);
    }

    TRule_sql_query rewrittenQuery;
    NYql::TIssues rewrittenIssues;
    if (!NYdb::NDump::SqlToProtoAst(rewritten, rewrittenQuery, rewrittenIssues)) {
        issues.AddIssues(rewrittenIssues);
        return Fail(issues, "rewritten table backup SQL is invalid");
    }

    TVector<TString> rewrittenStatements;
    if (!SplitStatements(rewritten, rewrittenStatements, issues)) {
        return false;
    }
    if (rewrittenStatements.size() != statements.size()) {
        return Fail(issues, TStringBuilder()
            << "SQL statement accounting mismatch: parsed " << statements.size()
            << ", split " << rewrittenStatements.size());
    }

    plan.SetFormatVersion(TableSchemaPlanFormatVersion);
    plan.SetSource(TPlan::SCHEMA_SOURCE_SQL);
    plan.SetRewrittenScript(rewritten);
    plan.SetPhase(TPlan::PHASE_BEFORE_DATA);
    for (ui32 ordinal = 0; ordinal < statements.size(); ++ordinal) {
        auto* output = plan.AddStatements();
        output->SetOrdinal(ordinal);
        output->SetKind(classified[ordinal].Kind);
        output->SetPhase(classified[ordinal].Phase);
        output->SetTarget(absoluteTargets[ordinal]);
        output->SetSql(rewrittenStatements[ordinal]);
        output->SetPopulationDependentIndexSettings(
            classified[ordinal].PopulationDependentIndexSettings);
        if (classified[ordinal].IndexName) {
            output->SetIndexName(indexNames[ordinal]);
        }
    }
    return true;
}

bool NormalizeSqlTableCreate(
    NKikimrSchemeOp::TModifyScheme& operation,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error)
{
    if (itemIdx >= importInfo.Items.size()) {
        error = "table SQL item index is out of range";
        return false;
    }
    const auto& item = importInfo.Items[itemIdx];
    if (!item.Table || !item.UsesSqlSchema()) {
        error = "table SQL create normalization requires an SQL table item";
        return false;
    }

    TString workingDir;
    TString name;
    if (!SplitDestination(item.DstPathName, workingDir, name, error)) {
        return false;
    }

    const TString preparedTarget = OperationTarget(operation);
    if (preparedTarget != item.DstPathName) {
        error = TStringBuilder() << "prepared CREATE TABLE targets " << preparedTarget.Quote()
            << ", expected " << item.DstPathName.Quote();
        return false;
    }

    operation.SetWorkingDir(workingDir);
    operation.SetInternal(true);
    switch (operation.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateTable:
            operation.MutableCreateTable()->SetName(name);
            operation.MutableCreateTable()->SetIsRestore(true);
            return true;
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: {
            auto* create = operation.MutableCreateIndexedTable();
            create->MutableTableDescription()->SetName(name);
            create->MutableTableDescription()->SetIsRestore(true);
            if (NeedToBuildIndexes(importInfo, itemIdx)) {
                auto* indexes = create->MutableIndexDescription();
                for (int index = indexes->size() - 1; index >= 0; --index) {
                    if (!IsLocalIndexType(indexes->Get(index).GetType())) {
                        indexes->DeleteSubrange(index, 1);
                    }
                }
            }
            return true;
        }
        case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
            operation.MutableCreateColumnTable()->SetName(name);
            operation.MutableCreateColumnTable()->SetIsRestore(true);
            return true;
        default:
            error = TStringBuilder() << "unexpected table create operation type: "
                << static_cast<ui32>(operation.GetOperationType());
            return false;
    }
}

bool ValidateSqlTableOperation(
    const NKikimrSchemeOp::TModifyScheme& operation,
    const TPlan::TStatement& statement,
    TString& error)
{
    const TString target = PreparedOperationTarget(operation);
    if (target.empty()) {
        error = TStringBuilder() << "cannot determine target of prepared operation "
            << static_cast<ui32>(operation.GetOperationType());
        return false;
    }

    const TString& expected = statement.GetTarget();
    const TString expectedPrefix = TStringBuilder() << expected << '/';
    const TString selectedIndexTarget = TStringBuilder()
        << expectedPrefix << statement.GetIndexName();
    const bool mayTargetChild = statement.GetKind() == TPlan::STATEMENT_KIND_ALTER_TABLE
        && IsIn({
            NKikimrSchemeOp::ESchemeOpCreateTableIndex,
            NKikimrSchemeOp::ESchemeOpAlterTableIndex,
        }, operation.GetOperationType());
    const bool targetsDirectChild = mayTargetChild
        && target.StartsWith(expectedPrefix)
        && target.size() > expectedPrefix.size()
        && target.find('/', expectedPrefix.size()) == TString::npos
        && (statement.GetIndexName().empty()
            || target == selectedIndexTarget);
    const TString indexImplPrefix = TStringBuilder()
        << selectedIndexTarget << '/';
    const bool targetsSelectedIndexImplTable = statement.GetPopulationDependentIndexSettings()
        && operation.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
        && !statement.GetIndexName().empty()
        && target.StartsWith(indexImplPrefix)
        && target.size() > indexImplPrefix.size()
        && target.find('/', indexImplPrefix.size()) == TString::npos;
    if (target != expected && !targetsDirectChild && !targetsSelectedIndexImplTable) {
        error = TStringBuilder() << "prepared operation targets " << target.Quote()
            << ", outside statement target " << expected.Quote();
        return false;
    }
    return true;
}

} // namespace NKikimr::NSchemeShard

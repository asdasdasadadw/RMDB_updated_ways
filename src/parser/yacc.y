%{
#include "ast.h"
#include "errors.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace ast;
%}

// request a pure (reentrant) parser
%define api.pure full
// enable location in error handler
%locations
// enable verbose syntax error message
%define parse.error verbose

// keywords
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC ORDER BY UNION
WHERE GROUP HAVING LIMIT UPDATE SET SELECT COUNT MAX MIN SUM AVG INT CHAR FLOAT INDEX AND JOIN ON EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ORDER_BY ENABLE_NESTLOOP ENABLE_SORTMERGE EXPLAIN ANALYZE AS
%token TRANSACTION ISOLATION LEVEL SNAPSHOT SERIALIZABLE
// non-keywords
%token LEQ NEQ GEQ T_EOF

// type-specific tokens
%token <sv_str> IDENTIFIER VALUE_STRING
%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT
%token <sv_bool> VALUE_BOOL

// specify types for non-terminal symbol
%type <sv_node> stmt dbStmt ddl dml txnStmt setStmt selectStmt unionStmt
%type <sv_field> field
%type <sv_fields> fieldList
%type <sv_type_len> type
%type <sv_comp_op> op
%type <sv_expr> expr
%type <sv_val> value
%type <sv_vals> valueList
%type <sv_str> tbName colName optAlias
%type <sv_strs> colNameList
%type <sv_table_ref> tableRef
%type <sv_from> fromClause
%type <sv_col> col
%type <sv_cols> colList opt_group_clause
%type <sv_agg_expr> aggExpr
%type <sv_agg_func_type> aggFunc
%type <sv_select_item> selectItem
%type <sv_select_items> selectItemList selector
%type <sv_set_clause> setClause
%type <sv_set_clauses> setClauses
%type <sv_cond> condition
%type <sv_conds> whereClause optWhereClause opt_having_clause
%type <sv_orderby> order_clause
%type <sv_orderbys> opt_order_clause order_list
%type <sv_orderby_dir> opt_asc_desc
%type <sv_int> opt_limit_clause
%type <sv_setKnobType> set_knob_type
%type <sv_txn_isolation> isolation_level

%%
start:
        stmt ';'
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    |   setStmt
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   SHOW INDEX FROM tbName
    {
        $$ = std::make_shared<ShowIndex>($4);
    }
    ;

setStmt:
        SET set_knob_type '=' VALUE_BOOL
    {
        $$ = std::make_shared<SetStmt>($2, $4);
    }
    |   SET TRANSACTION ISOLATION LEVEL isolation_level
    {
        $$ = std::make_shared<SetTransactionIsolation>($5);
    }
    ;

ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = std::make_shared<CreateTable>($3, $5);
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>($3);
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>($2);
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<CreateIndex>($3, $5);
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<DropIndex>($3, $5);
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>($3, $6);
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>($3, $4);
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $5);
    }
    |   selectStmt
    |   EXPLAIN ANALYZE selectStmt
    {
        $$ = std::make_shared<ExplainAnalyze>(std::dynamic_pointer_cast<SelectStmt>($3));
    }
    ;

selectStmt:
        SELECT selector FROM fromClause optWhereClause opt_group_clause opt_having_clause opt_order_clause opt_limit_clause
    {
        auto conds = $5;
        conds.insert(conds.begin(), $4->conds.begin(), $4->conds.end());
        $$ = std::make_shared<SelectStmt>($2, $4->tabs, conds, $6, $7, $8, $9);
    }
    ;

fieldList:
        field
    {
        $$ = std::vector<std::shared_ptr<Field>>{$1};
    }
    |   fieldList ',' field
    {
        $$.push_back($3);
    }
    ;

colNameList:
        colName
    {
        $$ = std::vector<std::string>{$1};
    }
    | colNameList ',' colName
    {
        $$.push_back($3);
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>($1, $2);
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    ;

valueList:
        value
    {
        $$ = std::vector<std::shared_ptr<Value>>{$1};
    }
    |   valueList ',' value
    {
        $$.push_back($3);
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<IntLit>($1, yyvsp[0].sv_raw_str);
    }
    |   VALUE_FLOAT
    {
        $$ = std::make_shared<FloatLit>($1, yyvsp[0].sv_raw_str);
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>($1, yyvsp[0].sv_raw_str);
    }
    |   VALUE_BOOL
    {
        $$ = std::make_shared<BoolLit>($1, yyvsp[0].sv_raw_str);
    }
    ;

condition:
        expr op expr
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    ;

optWhereClause:
        /* epsilon */ { $$ = {}; }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

whereClause:
        condition 
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   whereClause AND condition
    {
        $$.push_back($3);
    }
    ;

col:
        tbName '.' colName
    {
        $$ = std::make_shared<Col>($1, $3);
    }
    |   colName
    {
        $$ = std::make_shared<Col>("", $1);
    }
    ;

colList:
        col
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
    }
    |   colList ',' col
    {
        $$.push_back($3);
    }
    ;

op:
        '='
    {
        $$ = SV_OP_EQ;
    }
    |   '<'
    {
        $$ = SV_OP_LT;
    }
    |   '>'
    {
        $$ = SV_OP_GT;
    }
    |   NEQ
    {
        $$ = SV_OP_NE;
    }
    |   LEQ
    {
        $$ = SV_OP_LE;
    }
    |   GEQ
    {
        $$ = SV_OP_GE;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   aggExpr
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{$1};
    }
    |   setClauses ',' setClause
    {
        $$.push_back($3);
    }
    ;

setClause:
        colName '=' value
    {
        $$ = std::make_shared<SetClause>($1, $3);
    }
    |   colName '=' colName '+' value
    {
        $$ = std::make_shared<SetClause>($1, $3, '+', $5);
    }
    |   colName '=' colName '-' value
    {
        $$ = std::make_shared<SetClause>($1, $3, '-', $5);
    }
    |   colName '=' colName '*' value
    {
        $$ = std::make_shared<SetClause>($1, $3, '*', $5);
    }
    |   colName '=' colName '/' value
    {
        $$ = std::make_shared<SetClause>($1, $3, '/', $5);
    }
    |   colName '=' colName value
    {
        if ($4->raw_text.empty() || ($4->raw_text[0] != '+' && $4->raw_text[0] != '-')) {
            throw RMDBError("Expected arithmetic operator in UPDATE SET clause");
        }
        $$ = std::make_shared<SetClause>($1, $3, '+', $4);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   selectItemList
    ;

selectItemList:
        selectItem
    {
        $$ = std::vector<std::shared_ptr<SelectItem>>{$1};
    }
    |   selectItemList ',' selectItem
    {
        $$.push_back($3);
    }
    ;

selectItem:
        expr optAlias
    {
        $$ = std::make_shared<SelectItem>($1, $2);
    }
    ;

aggExpr:
        aggFunc '(' '*' ')'
    {
        $$ = std::make_shared<AggExpr>($1, true, nullptr);
    }
    |   aggFunc '(' col ')'
    {
        $$ = std::make_shared<AggExpr>($1, false, $3);
    }
    ;

aggFunc:
        COUNT
    {
        $$ = AGG_FUNC_COUNT;
    }
    |   MAX
    {
        $$ = AGG_FUNC_MAX;
    }
    |   MIN
    {
        $$ = AGG_FUNC_MIN;
    }
    |   SUM
    {
        $$ = AGG_FUNC_SUM;
    }
    |   AVG
    {
        $$ = AGG_FUNC_AVG;
    }
    ;

tableRef:
        tbName optAlias
    {
        $$ = std::make_shared<TableRef>($1, $2);
    }
    |   '(' unionStmt ')' optAlias
    {
        if ($4.empty()) {
            throw RMDBError("Derived table requires alias");
        }
        $$ = std::make_shared<TableRef>(std::dynamic_pointer_cast<UnionStmt>($2), $4);
    }
    ;

unionStmt:
        selectStmt UNION selectStmt
    {
        std::vector<std::shared_ptr<SelectStmt>> selects;
        selects.push_back(std::dynamic_pointer_cast<SelectStmt>($1));
        selects.push_back(std::dynamic_pointer_cast<SelectStmt>($3));
        $$ = std::make_shared<UnionStmt>(std::move(selects));
    }
    |   unionStmt UNION selectStmt
    {
        auto union_stmt = std::dynamic_pointer_cast<UnionStmt>($1);
        union_stmt->selects.push_back(std::dynamic_pointer_cast<SelectStmt>($3));
        $$ = union_stmt;
    }
    ;

optAlias:
        /* epsilon */
    {
        $$ = "";
    }
    |   AS IDENTIFIER
    {
        $$ = $2;
    }
    |   IDENTIFIER
    {
        $$ = $1;
    }
    ;

fromClause:
        tableRef
    {
        $$ = std::make_shared<FromClause>();
        $$->tabs.push_back($1);
    }
    |   fromClause ',' tableRef
    {
        $$ = $1;
        $$->tabs.push_back($3);
    }
    |   fromClause JOIN tableRef
    {
        $$ = $1;
        $$->tabs.push_back($3);
    }
    |   fromClause JOIN tableRef ON whereClause
    {
        $$ = $1;
        $$->tabs.push_back($3);
        $$->conds.insert($$->conds.end(), $5.begin(), $5.end());
    }
    ;

opt_order_clause:
    ORDER BY order_list
    { 
        $$ = $3; 
    }
    |   /* epsilon */ { $$ = {}; }
    ;

order_list:
      order_clause
    {
        $$ = std::vector<std::shared_ptr<OrderBy>>{$1};
    }
    | order_list ',' order_clause
    {
        $$.push_back($3);
    }
    ;

order_clause:
      expr  opt_asc_desc 
    { 
        $$ = std::make_shared<OrderBy>($1, $2);
    }
    ;   

opt_asc_desc:
    ASC          { $$ = OrderBy_ASC;     }
    |  DESC      { $$ = OrderBy_DESC;    }
    |       { $$ = OrderBy_DEFAULT; }
    ;    

opt_group_clause:
    GROUP BY colList
    {
        $$ = $3;
    }
    |   /* epsilon */ { $$ = {}; }
    ;

opt_having_clause:
    HAVING whereClause
    {
        $$ = $2;
    }
    |   /* epsilon */ { $$ = {}; }
    ;

opt_limit_clause:
    LIMIT VALUE_INT
    {
        $$ = $2;
    }
    |   /* epsilon */ { $$ = -1; }
    ;

set_knob_type:
    ENABLE_NESTLOOP { $$ = EnableNestLoop; }
    |   ENABLE_SORTMERGE { $$ = EnableSortMerge; }
    ;

isolation_level:
    SNAPSHOT ISOLATION { $$ = SnapshotIsolation; }
    |   SERIALIZABLE { $$ = Serializable; }
    ;

tbName: IDENTIFIER;

colName: IDENTIFIER;
%%

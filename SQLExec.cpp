/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @authors Kevin Lundeen, Justin Thoreson
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream& operator<<(ostream& out, const QueryResult& qres) {
    if (qres.column_names != nullptr) {
        for (Identifier& column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (ValueDict* row: *qres.rows) {
            for (Identifier& column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (this->column_names)
        delete this->column_names;
    if (this->column_attributes)
        delete this->column_attributes;
    if (this->rows) {
        for (ValueDict* row: *this->rows)
            delete row;
        delete this->rows;
    }
}

QueryResult* SQLExec::execute(const SQLStatement* statement) {
    if (!SQLExec::tables)
        SQLExec::tables = new Tables();
    if (!SQLExec::indices)
        SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement*) statement);
            case kStmtDrop:
                return drop((const DropStatement*) statement);
            case kStmtShow:
                return show((const ShowStatement*) statement);
            case kStmtInsert:
                return insert((const InsertStatement*) statement);
            case kStmtDelete:
                return del((const DeleteStatement*) statement);
            case kStmtSelect:
                return select((const SelectStatement*) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError("DbRelationError: " + string(e.what()));
    }
}

QueryResult* SQLExec::insert(const InsertStatement* statement) {
    Identifier table_name = statement->tableName;
    DbRelation& table = SQLExec::tables->get_table(table_name);
    ValueDict row;
    size_t columns_n = statement->columns->size();
    for (size_t i = 0; i < columns_n; i++) {
        Identifier column = (*statement->columns)[i];
        Expr* value = (*statement->values)[i];
        switch (value->type) {
            case kExprLiteralInt:
                row[column] = Value(value->ival);
                break;
            case kExprLiteralString:
                row[column] = Value(value->name);
                break;
            default:
                throw SQLExecError("column attribute unrecognized");
        }
    }
    Handle insertion = table.insert(&row);
    IndexNames indices = SQLExec::indices->get_index_names(table_name);
    for (const Identifier& idx : indices) {
        DbIndex &index = SQLExec::indices->get_index(table_name, idx);
        index.insert(insertion);
    }
    string suffix = indices.size() ? " and from " + to_string(indices.size()) + (indices.size() > 1 ? " indices" : " index") : "";
    return new QueryResult("successfully inserted 1 row into " + table_name + suffix);
}

void get_where_conjunction(const Expr* where, ValueDict* conjunction) {
    if (where->opType == Expr::OperatorType::AND) {
        get_where_conjunction(where->expr, conjunction);
        get_where_conjunction(where->expr2, conjunction);
    } else if (where->opType == Expr::OperatorType::SIMPLE_OP && where->opChar == '=') {
        switch (where->expr2->type) {
            case kExprLiteralInt:
                (*conjunction)[where->expr->name] = Value(where->expr2->ival);
                break;
            case kExprLiteralString:
                (*conjunction)[where->expr->name] = Value(where->expr2->name);
                break;
            default:
                throw SQLExecError("unrecognized expression");
        }
    }
}

ValueDict* get_where_conjunction(const Expr* where) {
    ValueDict* conjunction = new ValueDict();
    get_where_conjunction(where, conjunction);
    return conjunction;
}


QueryResult* SQLExec::del(const DeleteStatement* statement) {
    return new QueryResult("DELETE statement not yet implemented");  // FIXME
}

QueryResult* SQLExec::select(const SelectStatement* statement) {
    Identifier table_name = statement->fromTable->getName();
    DbRelation& table = SQLExec::tables->get_table(table_name);
    ColumnNames* cn = new ColumnNames();
    for (const Expr* expr : *statement->selectList) {
        if (expr->type == kExprStar)
            for (const Identifier& col : table.get_column_names())
                cn->push_back(col);
        else
            cn->push_back(expr->name);
    }

    // start base of plan at a TableScan
    EvalPlan* plan = new EvalPlan(table);

    // enclose in selection if where clause exists
    if (statement->whereClause)
        plan = new EvalPlan(get_where_conjunction(statement->whereClause), plan);
    
    // wrap in project
    plan = new EvalPlan(cn, plan);

    // optimize and evaluate
    plan = plan->optimize();
    ValueDicts* rows = plan->evaluate();
    delete plan;
    return new QueryResult(cn, table.get_column_attributes(*cn), rows, "successfully return " + to_string(rows->size()) + " rows");
}

void SQLExec::column_definition(const ColumnDefinition* col, Identifier& column_name, ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::DataType::INT:
            column_attribute = ColumnAttribute::DataType::INT;
            break;
        case ColumnDefinition::DataType::TEXT:
            column_attribute = ColumnAttribute::DataType::TEXT;
            break;
        default:
            throw SQLExecError("not implemented");
    }
}

QueryResult* SQLExec::create(const CreateStatement* statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult* SQLExec::create_table(const CreateStatement* statement) {
    // update _tables schema
    ValueDict row = {{"table_name", Value(statement->tableName)}};
    Handle tableHandle = SQLExec::tables->insert(&row);
    try {
        // update _columns schema
        Handles columnHandles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (ColumnDefinition* column : *statement->columns) {
                Identifier cn;
                ColumnAttribute ca;
                column_definition(column, cn, ca);
                std::string type = ca.get_data_type() == ColumnAttribute::DataType::TEXT ? "TEXT" : "INT";
                ValueDict row = {
                    {"table_name", Value(statement->tableName)},
                    {"column_name", Value(cn)},
                    {"data_type", Value(type)}
                };
                columnHandles.push_back(columns.insert(&row));
            }

            // create table
            DbRelation& table = SQLExec::tables->get_table(statement->tableName);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        } catch (...) {
            // attempt to undo the insertions into _columns
            try {
                for (Handle& columnHandle : columnHandles)
                    columns.del(columnHandle);
            } catch (...) {}
            throw;
        }
    } catch (...) {
        // attempt to undo the insertion into _tables
        try {
            SQLExec::tables->del(tableHandle);
        } catch (...) {}
        throw;
    }

    return new QueryResult("created table " + string(statement->tableName));
}

QueryResult* SQLExec::create_index(const CreateStatement* statement) {
    DbRelation& table = SQLExec::tables->get_table(statement->tableName);

    // check that all the index columns exist in the table
    const ColumnNames& cn = table.get_column_names();
    for (char* column_name : *statement->indexColumns)
        if (find(cn.begin(), cn.end(), string(column_name)) == cn.end())
            throw SQLExecError("no such column " + string(column_name) + " in table " + statement->tableName);

    // insert a row for each column in index key into _indices
    ValueDict row = {
        {"table_name", Value(statement->tableName)},
        {"index_name", Value(statement->indexName)},
        {"column_name", Value("")},
        {"seq_in_index", Value()},
        {"index_type", Value(statement->indexType)},
        {"is_unique", Value(string(statement->indexType) == "BTREE")}
    };
    for (char* column_name : *statement->indexColumns) {
        row["column_name"] = Value(column_name);
        row["seq_in_index"].n += 1;
        SQLExec::indices->insert(&row);
    }

    // call get_index to get a reference to the new index and then invoke the create method on it
    DbIndex& index = SQLExec::indices->get_index(string(statement->tableName), string(statement->indexName));
    index.create();

    return new QueryResult("created index " + string(statement->indexName));
}

QueryResult* SQLExec::drop(const DropStatement* statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult* SQLExec::drop_table(const DropStatement* statement) {
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");
    
    // get table name
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME || table_name == Indices::TABLE_NAME)
        throw SQLExecError("Cannot drop a schema table!");
    ValueDict where = {{"table_name", Value(table_name)}};
    Handles* tabMeta = SQLExec::tables->select(&where);
    if (tabMeta->empty())
        throw SQLExecError("Attempting to drop non-existent table " + table_name);

    // before dropping the table, drop each index on the table
    Handles* selected = SQLExec::indices->select(&where);
    for (Handle& row : *selected)
        SQLExec::indices->del(row);
    delete selected;

    // remove columns    
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* rows = columns.select(&where);
    for (Handle& row : *rows)
        columns.del(row);
    delete rows;

    // remove table
    DbRelation& table = SQLExec::tables->get_table(table_name);
    table.drop();
    rows = SQLExec::tables->select(&where);
    SQLExec::tables->del(*rows->begin());
    delete rows;

    return new QueryResult("dropped table " + table_name);    
}

QueryResult* SQLExec::drop_index(const DropStatement* statement) {
    // call get_index to get a reference to the index and then invoke the drop method on it
    Identifier table_name = statement->name; 
    Identifier index_name = statement->indexName; 
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    ValueDict where = {
        {"table_name", Value(table_name)},
        {"index_name", Value(index_name)}
    };
    Handles* idxMeta = SQLExec::indices->select(&where);
    if (idxMeta->empty())
        throw SQLExecError("Attempting to drop non-existent index " + index_name + " on " + table_name);

    // remove all the rows from _indices for this index
    Handles* selected = SQLExec::indices->select(&where);
    for (Handle& row : *selected)
        SQLExec::indices->del(row);
    delete selected;

    return new QueryResult("dropped index " + index_name + " on " + table_name);
}

QueryResult* SQLExec::show(const ShowStatement* statement) {
    switch(statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult* SQLExec::show_tables() {
    // get column names and attributes
    ColumnNames* cn = new ColumnNames();
    ColumnAttributes* ca = new ColumnAttributes();
    SQLExec::tables->get_columns(Tables::TABLE_NAME, *cn, *ca);

    // get table names
    Handles* tables = SQLExec::tables->select();
    ValueDicts* rows = new ValueDicts();
    for (Handle& table : *tables) {
        ValueDict* row = SQLExec::tables->project(table, cn);
        Identifier table_name = (*row)["table_name"].s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete tables;
    return new QueryResult(cn, ca, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult* SQLExec::show_columns(const ShowStatement* statement) {
    ColumnNames* cn = new ColumnNames({"table_name", "column_name", "data_type"});
    ColumnAttributes* ca = new ColumnAttributes({ColumnAttribute(ColumnAttribute::DataType::TEXT)});
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    ValueDict where = {{"table_name", Value(statement->tableName)}};
    Handles* selected = columns.select(&where);
    ValueDicts* rows = new ValueDicts();
    for (Handle& row : *selected)
        rows->push_back(columns.project(row, cn));
    delete selected;
    return new QueryResult(cn, ca, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult* SQLExec::show_index(const ShowStatement* statement){
    ColumnNames* cn = new ColumnNames({
        "table_name", "index_name", "column_name",
        "seq_in_index", "index_type", "is_unique"
    });
    ColumnAttributes* ca = new ColumnAttributes({
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::INT),
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::BOOLEAN),
    });
    ValueDict where = {{"table_name", Value(statement->tableName)}};
    Handles* selected = SQLExec::indices->select(&where);
    ValueDicts* rows = new ValueDicts();
    for (Handle& row : *selected)
        rows->push_back(SQLExec::indices->project(row, cn));
    delete selected;
    return new QueryResult(cn, ca, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

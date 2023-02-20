/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
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
    delete this->column_names;
    delete this->column_attributes;
    delete this->rows;
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXME: initialize _tables table, if not yet present
    if (tables == nullptr) {
        tables = new Tables();
    }


    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch(col->type){
        case ColumnDefinition::DataType::INT:
            column_attribute = ColumnAttribute::DataType::INT;
            break;
        case ColumnDefinition::DataType::TEXT:
            column_attribute = ColumnAttribute::DataType::TEXT;
            break;
        default:
            throw DbRelationError("not implemented");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_table(statement);
    default:
        return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    ValueDict row = {};
    row["table_name"] = Value(statement->tableName);
    tables->insert(&row);

    Handles handles;
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    for (ColumnDefinition* column : *statement->columns) {
        Identifier columnName;
        ColumnAttribute columnAttribute;
        column_definition(column, columnName, columnAttribute);

        row["table_name"] = Value(statement->tableName);
        row["column_name"] = columnName;
        switch (columnAttribute.get_data_type()) {
            case ColumnAttribute::INT:
                row["data_type"] = Value("INT");
                break;
            case ColumnAttribute::TEXT:
                row["data_type"] = Value("TEXT");
                break;
            default:
                throw new DbException("Unsupported data type");
        }

        handles.push_back(columns.insert(&row));
    }

    tables->get_table(statement->tableName).create();

    return new QueryResult("created " + string(statement->tableName));
}

QueryResult *SQLExec::create_index(const CreateStatement * statement) {
    DbRelation &table = tables->get_table(statement->tableName);

    ValueDict row;
    row["table_name"] = Value(statement->tableName);
    row["index_name"] = Value(statement->indexName);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(statement->indexType == "BTREE");

    for (Identifier columnName : *statement->indexColumns) {
        row["columnName"] = Value(columnName);
        row["seq_in_index"].n += 1;
        indices->insert(&row);
    }
    
    indices->get_index(statement->tableName, statement->indexName).create();

    return new QueryResult("created index " + string(statement->indexName));
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    if (statement->name == Tables::TABLE_NAME || statement->name == Columns::TABLE_NAME) {
        throw DbRelationError("Cannot drop a schema table!");
    }

    // Get columns
    ValueDict where;
    where["table_name"] = Value(statement->name);
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = columns.select(&where);
    
    // Delete columns
    for (Handle handle: *handles) {
        columns.del(handle);
    }

    // Drop table
    tables->get_table(statement->name).drop();

    // Delete table
    tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult(nullptr, nullptr, nullptr, "dropped " + string(statement->name));
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::show_tables() {
    ColumnNames* columnNames = new ColumnNames();
    ColumnAttributes* columnAttributes = new ColumnAttributes();
    tables->get_columns(Tables::TABLE_NAME, *columnNames, *columnAttributes);

    Handles* handles = tables->select();
    ValueDicts* rows = new ValueDicts();
    for (Handle handle: *handles) {
        rows->push_back(tables->project(handle, columnNames));
    }
    return new QueryResult(columnNames, columnAttributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation& table = tables->get_table(Columns::TABLE_NAME);

    ColumnNames *columnNames = new ColumnNames();
    columnNames->push_back("table_name");
    columnNames->push_back("column_name");
    columnNames->push_back("data_type");

    ColumnAttributes* columnAttributes = new ColumnAttributes();
    columnAttributes->push_back(ColumnAttribute::TEXT);

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = table.select(&where);

    ValueDicts* rows = new ValueDicts();
    for (Handle handle: *handles) {
        rows->push_back(table.project(handle, columnNames));
    }

    return new QueryResult(columnNames, columnAttributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames* columnNames = new ColumnNames();
    columnNames->push_back("table_name");
    columnNames->push_back("index_name");
    columnNames->push_back("column_name");
    columnNames->push_back("seq_in_index");
    columnNames->push_back("index_type");
    columnNames->push_back("is_unique");

    ColumnAttributes* columnAttributes = new ColumnAttributes();
    columnAttributes->push_back(ColumnAttribute::TEXT);
    columnAttributes->push_back(ColumnAttribute::TEXT);
    columnAttributes->push_back(ColumnAttribute::TEXT);
    columnAttributes->push_back(ColumnAttribute::INT);
    columnAttributes->push_back(ColumnAttribute::TEXT);
    columnAttributes->push_back(ColumnAttribute::BOOLEAN);

    ValueDict where;
    where["table_name"] = string(statement->tableName);
    Handles* handles = indices->select(&where);

    ValueDicts* rows = new ValueDicts();
    for (Handle handle: *handles) {
        rows->push_back(indices->project(handle, columnNames));
    }

    return new QueryResult(columnNames, columnAttributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    // drop the index
    indices->get_index(statement->name, statement->indexName).drop();
    
    ValueDict where; 
    where["table_name"] = Value(statement->name);
    where["index_name"] = Value(statement->indexName);

    Handles *select = indices->select(&where);
    
    for (Handle &row : *select)
        indices->del(row);
    delete select;

    return new QueryResult("dropped index " + string(statement->indexName));
}

/**
 * @file HeapTable.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */
#include <cstring>
#include "HeapTable.h"

using namespace std;
typedef uint16_t u16;

/**
 * Constructor
 * @param table_name
 * @param column_names
 * @param column_attributes
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(
        table_name, column_names, column_attributes), file(table_name) {
}

/**
 * Execute: CREATE TABLE <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create() {
    file.create();
}

/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create_if_not_exists() {
    try {
        open();
    } catch (DbException &e) {
        create();
    }
}

/**
 * Execute: DROP TABLE <table_name>
 */
void HeapTable::drop() {
    file.drop();
}

/**
 * Open existing table. Enables: insert, update, delete, select, project
 */
void HeapTable::open() {
    file.open();
}

/**
 * Closes the table. Disables: insert, update, delete, select, project
 */
void HeapTable::close() {
    file.close();
}

/**
 * Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
 * @param row a dictionary with column name keys
 * @return the handle of the inserted row
 */
Handle HeapTable::insert(const ValueDict *row) {
    open();
    ValueDict *full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}

/**
 * Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned from an insert
 * or select).
 * @param handle the row to be updated
 * @param new_values a dictionary with column name keys
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Not implemented");
}

/**
 * Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned from an insert
 * or select).
 * @param handle the row to be deleted
 */
void HeapTable::del(const Handle handle) {
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

/**
 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE 1
 * @return a list of handles for qualifying rows
 */
Handles *HeapTable::select() {
    return select(nullptr);
}

/**
 * The select command
 * @param where ignored for now FIXME
 * @return list of handles of the selected rows
 */
Handles *HeapTable::select(const ValueDict *where) {
    open();
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (selected(handle, where))
                handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Project all columns from a given row.
 * @param handle row to be projected
 * @return a sequence of all values for handle
 */
ValueDict *HeapTable::project(Handle handle) {
    return project(handle, &this->column_names);
}

/**
 * Project given columns from a given row.
 * @param handle row to be projected
 * @param column_names of columns to be included in the result
 * @return a sequence of values for handle given by column_names
 */
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict *result = new ValueDict();
    for (auto const &column_name: *column_names) {
        if (row->find(column_name) == row->end())
            throw DbRelationError("table does not have column named '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete row;
    return result;
}

/**
 * Check if the given row is acceptable to insert.
 * @param row to be validated
 * @return the full row dictionary
 * @throws DbRelationError if not valid
 */
ValueDict *HeapTable::validate(const ValueDict *row) const {
    ValueDict *full_row = new ValueDict();
    for (auto const &column_name: this->column_names) {
        Value value;
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        else
            value = column->second;
        (*full_row)[column_name] = value;
    }
    return full_row;
}

/**
 * Appends a record to the file.
 * @param row to be appended
 * @return handle of newly inserted row
 */
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        // need a new block
        delete block;
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char *) data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), record_id);
}

/**
 * Figure out the bits to go into the file.
 * The caller is responsible for freeing the returned Dbt and its enclosed ret->get_data().
 * @param row data for the tuple
 * @return bits of the record as it should appear on disk
 */
Dbt *HeapTable::marshal(const ValueDict *row) const {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;

        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16 *) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

/**
 * Figure out the memory data structures from the given bits gotten from the file.
 * @param data file data for the tuple
 * @return row data for the tuple
 */
ValueDict *HeapTable::unmarshal(Dbt *data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char *) data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

/**
 * See if the row at the given handle satisfies the given where clause
 * @param handle  row to check
 * @param where   conditions to check
 * @return        true if conditions met, false otherwise
 */
bool HeapTable::selected(Handle handle, const ValueDict *where) {
    if (where == nullptr)
        return true;
    ValueDict *row = this->project(handle, where);
    return *row == *where;
}

/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict &row, int a, string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
}

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation &table, Handle handle, int a, string b) {
    ValueDict *result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
        delete result;
        return false;
    }
    value = (*result)["b"];
    delete result;
    return !(value.s != b);
}

/**
 * Testing function for heap storage engine.
 * @return true if the tests all succeeded
 */
bool test_heap_storage() {
    if (!test_slotted_page())
        return assertion_failure("slotted page tests failed");
    cout << endl << "slotted page tests ok" << endl;

    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exists ok" << endl;

    ValueDict row;
    string b = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles *handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
    delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "many inserts/select/projects ok" << endl;
    delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "del ok" << endl;
    table.drop();
    delete handles;
    return true;
}

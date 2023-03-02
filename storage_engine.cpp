/**
 * @file storage_engine.cpp - implementation of some of the abstract classes' methods
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#include <algorithm>
#include "storage_engine.h"

bool Value::operator==(const Value& other) const {
    if (this->data_type != other.data_type)
        return false;
    if (this->data_type == ColumnAttribute::INT)
        return this->n == other.n;
    return this->s == other.s;
}

bool Value::operator!=(const Value& other) const {
    return !(*this == other);
}

// Get only selected column attributes
ColumnAttributes* DbRelation::get_column_attributes(const ColumnNames& select_column_names) const {
    ColumnAttributes* ret = new ColumnAttributes();
    for (auto const& column_name: select_column_names) {
        auto it = std::find(this->column_names.begin(), this->column_names.end(), column_name);
        if (it == this->column_names.end()) {
            delete ret;
            throw DbRelationError("unknown column " + column_name);
        }
        ptrdiff_t index = it - this->column_names.begin();
        ret->push_back(this->column_attributes[index]);
    }
    return ret;
}

// Just pulls out the column names from a ValueDict and passes that to the usual form of project().
ValueDict* DbRelation::project(Handle handle, const ValueDict* where) {
    ColumnNames t;
    for (auto const& column: *where)
        t.push_back(column.first);
    return this->project(handle, &t);
}

// Do a projection for each of a list of handles
ValueDicts* DbRelation::project(Handles* handles) {
    ValueDicts* ret = new ValueDicts();
    for (auto const& handle: *handles)
        ret->push_back(project(handle));
    return ret;
}

// Do a projection for each of a list of handles
ValueDicts* DbRelation::project(Handles* handles, const ColumnNames* column_names) {
    ValueDicts* ret = new ValueDicts();
    for (auto const& handle: *handles)
        ret->push_back(project(handle, column_names));
    return ret;
}

// Do a projection for each of a list of handles
ValueDicts* DbRelation::project(Handles* handles, const ValueDict* where) {
    ColumnNames t;
    for (auto const& column: *where)
        t.push_back(column.first);
    ValueDicts* ret = new ValueDicts();
    for (auto const& handle: *handles)
        ret->push_back(project(handle, &t));
    return ret;
}
/**
 * @file HeapTable.h - Implementation of storage_engine with a heap file structure.
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include "storage_engine.h"
#include "SlottedPage.h"
#include "HeapFile.h"

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

class HeapTable : public DbRelation {
public:
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable &other) = delete;

    HeapTable(HeapTable &&temp) = delete;

    HeapTable &operator=(const HeapTable &other) = delete;

    HeapTable &operator=(HeapTable &&temp) = delete;

    virtual void create();

    virtual void create_if_not_exists();

    virtual void drop();

    virtual void open();

    virtual void close();

    virtual Handle insert(const ValueDict *row);

    virtual void update(const Handle handle, const ValueDict *new_values);

    virtual void del(const Handle handle);

    virtual Handles *select();

    virtual Handles *select(const ValueDict *where);

    virtual ValueDict *project(Handle handle);

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

    using DbRelation::project;

protected:
    HeapFile file;

    virtual ValueDict *validate(const ValueDict *row) const;

    virtual Handle append(const ValueDict *row);

    virtual Dbt *marshal(const ValueDict *row) const;

    virtual ValueDict *unmarshal(Dbt *data) const;

    virtual bool selected(Handle handle, const ValueDict *where);
};

bool test_heap_storage();


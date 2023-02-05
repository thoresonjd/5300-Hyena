/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023":
 */
#pragma once

#include "storage_engine.h"

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.

        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */
class SlottedPage : public DbBlock {
public:
    SlottedPage(Dbt &block, BlockID block_id, bool is_new = false);

    // Big 5 - use the defaults
    virtual ~SlottedPage() {}

    virtual RecordID add(const Dbt *data);

    virtual Dbt *get(RecordID record_id) const;

    virtual void put(RecordID record_id, const Dbt &data);

    virtual void del(RecordID record_id);

    virtual RecordIDs *ids(void) const;

protected:
    uint16_t num_records;
    uint16_t end_free;

    void get_header(uint16_t &size, uint16_t &loc, RecordID id = 0) const;

    void put_header(RecordID id = 0, uint16_t size = 0, uint16_t loc = 0);

    bool has_room(uint16_t size) const;

    virtual void slide(uint16_t start, uint16_t end);

    uint16_t get_n(uint16_t offset) const;

    void put_n(uint16_t offset, uint16_t n);

    void *address(uint16_t offset) const;

    friend bool test_slotted_page();
};

bool assertion_failure(std::string message, double x = -1, double y = -1);
bool test_slotted_page();

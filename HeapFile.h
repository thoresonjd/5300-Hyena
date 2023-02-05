/*
 * @file HeapFile.h - Implementation of storage_engine with a heap file structure.
 * HeapFile: DbFile
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include "db_cxx.h"
#include "SlottedPage.h"


/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    HeapFile(std::string name);

    virtual ~HeapFile() {}

    HeapFile(const HeapFile &other) = delete;

    HeapFile(HeapFile &&temp) = delete;

    HeapFile &operator=(const HeapFile &other) = delete;

    HeapFile &operator=(HeapFile &&temp) = delete;

    virtual void create(void);

    virtual void drop(void);

    virtual void open(void);

    virtual void close(void);

    virtual SlottedPage *get_new(void);

    virtual SlottedPage *get(BlockID block_id);

    virtual void put(DbBlock *block);

    virtual BlockIDs *block_ids() const;

    /**
     * Get the id of the current final block in the heap file.
     * @return block id of last block
     */
    virtual uint32_t get_last_block_id() { return last; }

protected:
    std::string dbfilename;
    uint32_t last;
    bool closed;
    Db db;

    virtual void db_open(uint flags = 0);

    virtual uint32_t get_block_count();
};


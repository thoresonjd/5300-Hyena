/**
 * @file HeapFile.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */
#include <cstring>
#include "db_cxx.h"
#include "HeapFile.h"

using namespace std;
typedef uint16_t u16;

/**
 * Constructor
 * @param name
 */
HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    this->dbfilename = this->name + ".db";
}

/**
 * Create physical file.
 */
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = get_new(); // force one page to exist
    delete page;
}

/**
 * Delete the physical file.
 */
void HeapFile::drop(void) {
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

/**
 * Open physical file.
 */
void HeapFile::open(void) {
    db_open();
}

/**
 * Close the physical file.
 */
void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

/**
 * Allocate a new block for the database file.
 * @return the new empty DbBlock that is managing the records in this block and its block id.
 */
SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization done to it
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last);
}

/**
 * Get a block from the database file.
 * @param block_id
 * @return          the given slotted page (freed by caller)
 */
SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

/**
 * Write a block back to the database file.
 * @param block
 */
void HeapFile::put(DbBlock *block) {
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Sequence of all block ids.
 * @return block ids
 */
BlockIDs *HeapFile::block_ids() const {
    BlockIDs *vec = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        vec->push_back(block_id);
    return vec;
}

/**
 * Ask BerkDb how many blocks we are currently using in the file.
 * @return number of blocks
 */
uint32_t HeapFile::get_block_count() {
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    free(stat);
    return bt_ndata;
}

/**
 * Wrapper for Berkeley DB open, which does both open and creation.
 * @param flags BerkDb flags
 */
void HeapFile::db_open(uint flags) {
    if (!this->closed)
        return;
    this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

    this->last = flags ? 0 : get_block_count();
    this->closed = false;
}

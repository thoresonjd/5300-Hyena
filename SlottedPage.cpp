/**
 * @file SlottedPage.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */
#include <cstring>
#include "SlottedPage.h"

using namespace std;
typedef uint16_t u16;

/**
 * SlottedPage constructor
 * @param block
 * @param block_id
 * @param is_new
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Add a new record to the block.
 * @param data
 * @return the new block's id
 */
RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room((u16) data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * Get a record from the block.
 * @param record_id
 * @return the bits of the record as stored in the block, or nullptr if it has been deleted (freed by caller)
 */
Dbt *SlottedPage::get(RecordID record_id) const {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
        return nullptr;  // this is just a tombstone, record has been deleted
    return new Dbt(this->address(loc), size);
}

/**
 * Replace the record with the given data.
 * @param record_id   record to replace
 * @param data        new contents of record_id
 * @throws DbBlockNoRoomError if it won't fit
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for enlarged record");
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

/**
 * Delete a record from the page.
 *
 * Mark the given id as deleted by changing its size to zero and its location to 0.
 * Compact the rest of the data in the block. But keep the record ids the same for everyone.
 *
 * @param record_id  record to delete
 */
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);  // 0 is the tombstone sentinel
    slide(loc, loc + size);
}

/**
 * Sequence of all non-deleted record IDs.
 * @return  sequence of IDs (freed by caller)
 */
RecordIDs *SlottedPage::ids(void) const {
    RecordIDs *vec = new RecordIDs();
    u16 size, loc;
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
        get_header(size, loc, record_id);
        if (loc != 0)
            vec->push_back(record_id);
    }
    return vec;
}

/**
 * Get the size and offset for given id. For id of zero, it is the block header.
 * @param size  set to the size from given header
 * @param loc   set to the byte offset from given header
 * @param id    the id of the header to fetch
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) const {
    size = get_n((u16) 4 * id);
    loc = get_n((u16) (4 * id + 2));
}

/**
 * Store the size and offset for given id. For id of zero, store the block header.
 * @param id
 * @param size
 * @param loc
 */
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16) 4 * id, size);
    put_n((u16) (4 * id + 2), loc);
}

/**
 * Calculate if we have room to store a record with given size. The size should include the 4 bytes
 * for the header, too, if this is an add.
 * @param size   size of the new record (not including the header space needed)
 * @return       true if there is enough room, false otherwise
 */
bool SlottedPage::has_room(u16 size) const {
    return 4 * (this->num_records + 1) + size <= this->end_free;
}

/**
 * Slide the contents to compensate for a smaller/larger record.
 *
 * If start < end, then remove data from offset start up to but not including offset end by sliding data
 * that is to the left of start to the right. If start > end, then make room for extra data from end to start
 * by sliding data that is to the left of start to the left.
 * Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
 * shift (end < start).
 *
 * @param start  beginning of slide
 * @param end    end of slide
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void *to = this->address((u16) (this->end_free + 1 + shift));
    void *from = this->address((u16) (this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    memmove(to, from, bytes);

    // fix up headers to the right
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids) {
        u16 size, loc;
        get_header(size, loc, record_id);
        if (loc <= start) {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}

/**
 * Get 2-byte integer at given offset in block.
 */
u16 SlottedPage::get_n(u16 offset) const {
    return *(u16 *) this->address(offset);
}

/**
 * Put a 2-byte integer at given offset in block.
 * @param offset number of bytes into the page
 * @param n
 */
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *) this->address(offset) = n;
}

/**
 * Make a void* pointer for a given offset into the data block.
 * @param offset
 * @return
 */
void *SlottedPage::address(u16 offset) const {
    return (void *) ((char *) this->block.get_data() + offset);
}

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(string message, double x, double y) {
    cout << "FAILED TEST: " << message;
    if (x >= 0)
        cout << " " << x;
    if (y >= 0)
        cout << " " << y;
    cout << endl;
    return false;
}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise
 */
bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);

    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");

    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);

    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");

    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);

    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);

    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);

    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");

    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }

    // more volume
    string gettysburg = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    int32_t n = -1;
    uint16_t text_length = gettysburg.size();
    uint total_size = sizeof(n) + sizeof(text_length) + text_length;
    char *data = new char[total_size];
    *(int32_t *) data = n;
    *(uint16_t *) (data + sizeof(n)) = text_length;
    memcpy(data + sizeof(n) + sizeof(text_length), gettysburg.c_str(), text_length);
    Dbt dbt(data, total_size);
    vector<SlottedPage> page_list;
    BlockID block_id = 1;
    Dbt slot_dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
    slot = SlottedPage(slot_dbt, block_id++, true);
    for (int i = 0; i < 10000; i++) {
        try {
            slot.add(&dbt);
        } catch (DbBlockNoRoomError &exc) {
            page_list.push_back(slot);
            slot_dbt = Dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
            slot = SlottedPage(slot_dbt, block_id++, true);
            slot.add(&dbt);
        }
    }
    page_list.push_back(slot);
    for (const auto &slot : page_list) {
        RecordIDs *ids = slot.ids();
        for (RecordID id : *ids) {
            Dbt *record = slot.get(id);
            if (record->get_size() != total_size)
                return assertion_failure("more volume wrong size", block_id - 1, id);
            void *stored = record->get_data();
            if (memcmp(stored, data, total_size) != 0)
                return assertion_failure("more volume wrong data", block_id - 1, id);
            delete record;
        }
        delete ids;
        delete[] (char *) slot.block.get_data();  // this is why we need to be a friend--just convenient
    }
    delete[] data;
    return true;
}
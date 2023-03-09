/**
 * @file BTreeNode.h - BTreeNode class and its subclasses: BTreeStat, BTreeInterior, BTreeLeaf
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include "storage_engine.h"
#include "heap_storage.h"

using KeyProfile = std::vector<ColumnAttribute::DataType>;
using KeyValue = std::vector<Value>;
using KeyValues = std::vector<KeyValue *>;
using BlockPointers = std::vector<BlockID>;
using Insertion = std::pair<BlockID, KeyValue>;

class BTreeNode {
public:
    BTreeNode(HeapFile &file, BlockID block_id, const KeyProfile &key_profile, bool create);

    virtual ~BTreeNode();

    static bool insertion_is_none(Insertion insertion) { return insertion.first == 0; }

    static Insertion insertion_none() { return Insertion(0, KeyValue()); }

    virtual void save();

    BlockID get_id() const { return this->id; }

protected:
    SlottedPage *block;
    HeapFile &file;
    BlockID id;
    const KeyProfile &key_profile;

    static Dbt *marshal_block_id(BlockID block_id);

    static Dbt *marshal_handle(Handle handle);

    virtual Dbt *marshal_key(const KeyValue *key);

    virtual BlockID get_block_id(RecordID record_id) const;

    virtual Handle get_handle(RecordID record_id) const;

    virtual KeyValue *get_key(RecordID record_id) const;
};

class BTreeStat : public BTreeNode {
public:
    static const RecordID ROOT = 1;  // where we store the root id in the stat block
    static const RecordID HEIGHT = ROOT + 1;  // where we store the height in the stat block

    BTreeStat(HeapFile &file, BlockID stat_id, BlockID new_root, const KeyProfile &key_profile);

    BTreeStat(HeapFile &file, BlockID stat_id, const KeyProfile &key_profile);

    virtual ~BTreeStat() {}

    virtual void save();

    BlockID get_root_id() const { return this->root_id; }

    void set_root_id(BlockID root_id) { this->root_id = root_id; }

    uint get_height() const { return this->height; }

    void set_height(uint height) { this->height = height; }

protected:
    BlockID root_id;
    uint height;

};

class BTreeInterior : public BTreeNode {
public:
    BTreeInterior(HeapFile &file, BlockID block_id, const KeyProfile &key_profile, bool create);

    virtual ~BTreeInterior();

    BTreeNode *find(const KeyValue *key, uint depth) const;

    Insertion insert(const KeyValue *boundary, BlockID block_id);

    virtual void save();

    void set_first(BlockID first) { this->first = first; }

    friend std::ostream &operator<<(std::ostream &out, const BTreeInterior &node);

protected:
    BlockID first;
    BlockPointers pointers;
    KeyValues boundaries;
};

class BTreeLeaf : public BTreeNode {
public:
    BTreeLeaf(HeapFile &file, BlockID block_id, const KeyProfile &key_profile, bool create);

    virtual ~BTreeLeaf();

    Handle find_eq(const KeyValue *key) const;  // throws if not found
    Insertion insert(const KeyValue *key, Handle handle);

    virtual void save();

protected:
    BlockID next_leaf;
    std::map<KeyValue, Handle> key_map;
};


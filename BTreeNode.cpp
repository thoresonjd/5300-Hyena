/**
 * @file BTreeNode.cpp - implementation of BTreeNode classes
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#include <cstring>
#include "BTreeNode.h"

using namespace std;

/************************
 * BTreeNode base class *
 ************************/

BTreeNode::BTreeNode(HeapFile &file, BlockID block_id, const KeyProfile &key_profile, bool create) : block(nullptr),
                                                                                                     file(file),
                                                                                                     id(block_id),
                                                                                                     key_profile(
                                                                                                             key_profile) {
    if (create) {
        this->block = file.get_new();
        this->id = this->block->get_block_id();
    } else {
        this->block = file.get(block_id);
    }
}

BTreeNode::~BTreeNode() {
    delete this->block;
    this->block = nullptr;
}

void BTreeNode::save() {
    this->file.put(this->block);
}

// Get the record and turn it into a block ID.
BlockID BTreeNode::get_block_id(RecordID record_id) const {
    Dbt *dbt = this->block->get(record_id);
    BlockID block_id = *(BlockID *) dbt->get_data();
    delete dbt;
    return block_id;
}

// Get the record and turn it into a Handle.
Handle BTreeNode::get_handle(RecordID record_id) const {
    Dbt *dbt = this->block->get(record_id);
    BlockID handle_block_id = *(BlockID *) dbt->get_data();
    RecordID handle_record_id = *(RecordID *) ((char *) dbt->get_data() + sizeof(BlockID));
    delete dbt;
    return Handle(handle_block_id, handle_record_id);
}

// Get the record and turn it into a KeyValue.
KeyValue *BTreeNode::get_key(RecordID record_id) const {
    Dbt *dbt = this->block->get(record_id);
    char *bytes = (char *) dbt->get_data();
    KeyValue *key_value = new KeyValue();
    Value value;
    uint offset = 0;
    for (auto const &data_type: this->key_profile) {
        value.data_type = data_type;
        if (data_type == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (data_type == ColumnAttribute::DataType::TEXT) {
            uint16_t size = *(uint16_t *) (bytes + offset);
            offset += sizeof(uint16_t);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = std::string(buffer);  // assume ascii for now
            offset += size;
        } else if (data_type == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t *) (bytes + offset);
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to unmarshal INT, TEXT, or BOOLEAN");
        }
        key_value->push_back(value);
    }
    delete dbt;
    return key_value;
}

// Convert block_id into bytes.
Dbt *BTreeNode::marshal_block_id(BlockID block_id) {
    char *bytes = new char[sizeof(BlockID)];
    Dbt *dbt = new Dbt(bytes, sizeof(BlockID));
    *(BlockID *) bytes = block_id;
    return dbt;
}

// Convert handle into bytes.
Dbt *BTreeNode::marshal_handle(Handle handle) {
    char *bytes = new char[sizeof(BlockID) + sizeof(RecordID)];
    Dbt *dbt = new Dbt(bytes, sizeof(BlockID) + sizeof(RecordID));
    *(BlockID *) bytes = handle.first;
    *(RecordID *) (bytes + sizeof(BlockID)) = handle.second;
    return dbt;
}

// Convert KeyValue into bytes.
Dbt *BTreeNode::marshal_key(const KeyValue *key) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need
    uint offset = 0;
    uint col_num = 0;
    for (auto const &data_type: this->key_profile) {
        Value value = (*key)[col_num];

        if (data_type == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("index key too big to marshal");

            *(int32_t *) (bytes + offset) = value.n;
            offset += sizeof(int32_t);

        } else if (data_type == ColumnAttribute::DataType::TEXT) {
            u_long size = (uint16_t) value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("index key too big to marshal");

            *(uint16_t *) (bytes + offset) = (uint16_t) size;
            offset += sizeof(uint16_t);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;

        } else if (data_type == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("index key too big to marshal");

            *(uint8_t *) (bytes + offset) = (uint8_t) value.n;
            offset += sizeof(uint8_t);

        } else {
            throw DbRelationError("only know how to marshal INT, TEXT, or BOOLEAN for BTree index");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}


/******************************
 * BTreeStat statistics block *
 ******************************/

BTreeStat::BTreeStat(HeapFile &file, BlockID stat_id, BlockID new_root, const KeyProfile &key_profile) : BTreeNode(file,
                                                                                                                   stat_id,
                                                                                                                   key_profile,
                                                                                                                   false),
                                                                                                         root_id(new_root),
                                                                                                         height(1) {
    save();
}

BTreeStat::BTreeStat(HeapFile &file, BlockID stat_id, const KeyProfile &key_profile) : BTreeNode(file, stat_id,
                                                                                                 key_profile, false),
                                                                                       root_id(get_block_id(ROOT)),
                                                                                       height(get_block_id(HEIGHT)) {
}

void BTreeStat::save() {
    Dbt *dbt = marshal_block_id(this->root_id);
    bool is_new = (this->block->size() == 0);
    if (is_new)
        this->block->add(dbt);
    else
        this->block->put(ROOT, *dbt);
    delete[] (char *) dbt->get_data();
    delete dbt;

    dbt = marshal_block_id(this->height);  // not really a block ID but it fits
    if (is_new)
        this->block->add(dbt);
    else
        this->block->put(HEIGHT, *dbt);
    delete[] (char *) dbt->get_data();
    delete dbt;

    BTreeNode::save();
}


/*****************
 * BTreeInterior *
 *****************/

BTreeInterior::BTreeInterior(HeapFile &file, BlockID block_id, const KeyProfile &key_profile, bool create) : BTreeNode(
        file, block_id, key_profile, create), first(0), pointers(), boundaries() {
    if (!create) {
        RecordIDs *record_id_list = this->block->ids();
        RecordID i = 1;
        for (auto j = record_id_list->size(); j > 0; j--) {
            if (i == 1) {
                // first pointer
                this->first = get_block_id(i);
            } else if (i % 2 != 0) {
                // pointer
                this->pointers.push_back(get_block_id(i));
            } else {
                // key
                KeyValue *key_value = get_key(i);
                this->boundaries.push_back(key_value);
            }
            i++;
        }
        delete record_id_list;
    }
}

BTreeInterior::~BTreeInterior() {
    for (auto key_value: this->boundaries)
        delete key_value;
    this->boundaries.clear();
}

// Get next block down in tree where key must be.
BTreeNode *BTreeInterior::find(const KeyValue *key, uint depth) const {
    BlockID down = this->pointers.back();  // last pointer is correct if we don't find an earlier boundary
    for (uint i = 0; i < this->boundaries.size(); i++) {
        KeyValue *boundary = this->boundaries[i];
        if (*boundary > *key) {
            if (i > 0)
                down = this->pointers[i - 1];
            else
                down = this->first;
            break;
        }
    }
    if (depth == 2)
        return new BTreeLeaf(this->file, down, this->key_profile, false);
    else
        return new BTreeInterior(this->file, down, this->key_profile, false);
}

// Save the pointers and boundaries in the correct order
void BTreeInterior::save() {
    Dbt *dbt;
    this->block->clear();
    dbt = marshal_block_id(this->first);
    this->block->add(dbt);
    delete[] (char *) dbt->get_data();
    delete dbt;
    for (uint i = 0; i < this->boundaries.size(); i++) {
        // key
        dbt = marshal_key(this->boundaries[i]);
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;

        // boundary
        dbt = marshal_block_id(this->pointers[i]);
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;
    }
    BTreeNode::save();
}

// Insert boundary, block_id pair into block.
Insertion BTreeInterior::insert(const KeyValue *boundary, BlockID block_id) {
    // cout << "inserting (" << block_id << ", " << (*boundary)[0] << ") into interior node " << id; // DEBUG
    // cout << " (pointers:" << boundaries.size() << ", unused:" << block->unused_bytes() << ") " << endl; // DEBUG

    Dbt *dbt;

    bool inserted = false;
    for (uint i = 0; i < this->boundaries.size(); i++) {
        KeyValue *check = this->boundaries[i];
        if (*boundary == *check) {
            this->boundaries.insert(this->boundaries.begin() + i, new KeyValue(*boundary));
            this->pointers.insert(this->pointers.begin() + i, block_id);
            inserted = true;
            break;
        }
    }
    if (!inserted) {
        // must go at the end
        this->boundaries.push_back(new KeyValue(*boundary));
        this->pointers.push_back(block_id);
    }
    dbt = marshal_block_id(block_id);
    try {
        // following is just a check for size (the save method will redo this in the right order)
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;
        dbt = marshal_key(boundary);
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;

        // that worked, so no need to split
        save();
        return BTreeNode::insertion_none();

    } catch (DbBlockNoRoomError &e) {
        cout << "splitting " << *this << endl; // DEBUG
        delete[] (char *) dbt->get_data();
        delete dbt;

        // too big, so split

        // create the sister
        BTreeInterior *nnode = new BTreeInterior(this->file, 0, this->key_profile, true);

        // only the pointer of the middle entry goes into the sister (as it's first pointer)
        // the corresponding boundary is moved up to be inserted into the parent node
        u_long split = this->boundaries.size() / 2;
        nnode->first = this->pointers[split];
        KeyValue *nboundary = this->boundaries[split];
        Insertion ret(nnode->id, *nboundary);
        delete nboundary;

        // move half of the entries to the sister
        for (u_long i = split + 1; i < this->boundaries.size(); i++) {
            nnode->boundaries.push_back(this->boundaries[i]);
            nnode->pointers.push_back(this->pointers[i]);
        }
        this->boundaries.erase(this->boundaries.begin() + split, this->boundaries.end());
        this->pointers.erase(this->pointers.begin() + split, this->pointers.end());
        // cout << "after split " << *this << endl; // DEBUG
        // cout << "new sibling " << *nnode << endl; // DEBUG

        // save everything
        nnode->save();
        this->save();
        return ret;
    }
}


ostream &operator<<(ostream &out, const BTreeInterior &node) {
    out << "(interior block " << node.id << "): " << node.first;
    if (node.boundaries.size() != node.pointers.size()) {
        out << " MISMATCH boundaries: " << node.boundaries.size() << ", pointers: " << node.pointers.size();
    } else {
        for (unsigned int i = 0; i < node.boundaries.size(); i++)
            out << '|' << (*node.boundaries[i])[0] << '|' << node.pointers[i];
    }
    return out;
}


/*************
 * BTreeLeaf *
 *************/

BTreeLeaf::BTreeLeaf(HeapFile &file, BlockID block_id, const KeyProfile &key_profile, bool create) : BTreeNode(file,
                                                                                                               block_id,
                                                                                                               key_profile,
                                                                                                               create),
                                                                                                     next_leaf(0),
                                                                                                     key_map() {
    if (!create) {
        RecordIDs *record_id_list = this->block->ids();
        RecordID i = 1;
        for (auto j = record_id_list->size(); j > 0; j--) {
            if (i == record_id_list->size()) {
                // next leaf block
                this->next_leaf = get_block_id(i);
            } else if (i % 2 == 0) {
                // record i-1: handle, record i: key
                KeyValue *key_value = get_key(i);
                this->key_map[*key_value] = get_handle(i - 1);
            }
            i++;
        }
        delete record_id_list;
    }
}

BTreeLeaf::~BTreeLeaf() {
}

// Find the handle for a given key
Handle BTreeLeaf::find_eq(const KeyValue *key) const {
    return this->key_map.at(*key);
}

// Save the key_map and next_leaf data in the correct order
void BTreeLeaf::save() {
    Dbt *dbt;
    this->block->clear();
    for (auto const &item: this->key_map) {
        // handle
        dbt = marshal_handle(item.second);
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;

        // key
        dbt = marshal_key(&item.first);
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;
    }
    // next leaf pointer is final record
    dbt = marshal_block_id(this->next_leaf);
    this->block->add(dbt);
    delete[] (char *) dbt->get_data();
    delete dbt;

    BTreeNode::save();
}

// Insert key, handle pair into block.
Insertion BTreeLeaf::insert(const KeyValue *key, Handle handle) {
    // cout << "inserting " << (*key)[0] << " into leaf " << id << endl; // DEBUG
    // check unique
    if (this->key_map.find(*key) != this->key_map.end())
        throw DbRelationError("Duplicate keys are not allowed in unique index");

    Dbt *dbt;
    dbt = marshal_handle(handle);
    try {
        // following is just a check for size (the save method will redo this in the right order)
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;
        dbt = marshal_key(key);
        this->block->add(dbt);
        delete[] (char *) dbt->get_data();
        delete dbt;

        // that worked, so no need to split
        this->key_map[*key] = handle;
        save();
        return BTreeNode::insertion_none();

    } catch (DbBlockNoRoomError &e) {
        delete[] (char *) dbt->get_data();
        delete dbt;

        // too big, so split

        // create the sister and put her to the right
        BTreeLeaf *nleaf = new BTreeLeaf(this->file, 0, this->key_profile, true);
        nleaf->next_leaf = this->next_leaf;
        this->next_leaf = nleaf->id;

        // move half of the entries to the sister
        auto key_list = this->key_map;       // make a copy of my key_map
        key_list[*key] = handle;             // add key/handle to it
        u_long split = key_list.size() / 2;  // figure out how many to keep (the rest move to nleaf)
        this->key_map.clear();               // empty my list
        u_long i = 0;
        KeyValue boundary;
        for (auto const &item: key_list) {
            if (i < split) {
                this->key_map[item.first] = item.second;
            } else if (i == split) {
                boundary = item.first;
                nleaf->key_map[boundary] = item.second;
            } else {
                nleaf->key_map[item.first] = item.second;
            }
            i++;
        }
        cout << "splitting leaf " << id << ", new sibling " << nleaf->id; // DEBUG
        cout << " starting at value " << boundary[0] << endl; // DEBUG

        nleaf->save();
        this->save();
        return Insertion(nleaf->id, boundary);
    }
}


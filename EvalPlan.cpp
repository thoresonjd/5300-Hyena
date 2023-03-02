/**
 * @file EvalPlan.cpp - implementation of evaluation plan classes
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#include "EvalPlan.h"


class Dummy : public DbRelation {
public:
    static Dummy &one() {
        static Dummy d;
        return d;
    }

    Dummy() : DbRelation("dummy", ColumnNames(), ColumnAttributes()) {}

    virtual void create() {};

    virtual void create_if_not_exists() {};

    virtual void drop() {};

    virtual void open() {};

    virtual void close() {};

    virtual Handle insert(const ValueDict *row) { return Handle(); }

    virtual void update(const Handle handle, const ValueDict *new_values) {}

    virtual void del(const Handle handle) {}

    virtual Handles *select() { return nullptr; };

    virtual Handles *select(const ValueDict *where) { return nullptr; }

    virtual Handles *select(Handles *current_selection, const ValueDict *where) { return nullptr; }

    virtual ValueDict *project(Handle handle) { return nullptr; }

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names) { return nullptr; }
};

EvalPlan::EvalPlan(PlanType type, EvalPlan *relation) : type(type), relation(relation), projection(nullptr),
                                                        select_conjunction(nullptr), table(Dummy::one()) {
}

EvalPlan::EvalPlan(ColumnNames *projection, EvalPlan *relation) : type(Project), relation(relation),
                                                                  projection(projection), select_conjunction(nullptr),
                                                                  table(Dummy::one()) {
}

EvalPlan::EvalPlan(ValueDict *conjunction, EvalPlan *relation) : type(Select), relation(relation), projection(nullptr),
                                                                 select_conjunction(conjunction), table(Dummy::one()) {
}

EvalPlan::EvalPlan(DbRelation &table) : type(TableScan), relation(nullptr), projection(nullptr),
                                        select_conjunction(nullptr), table(table) {
}

EvalPlan::EvalPlan(const EvalPlan *other) : type(other->type), table(other->table) {
    if (other->relation != nullptr)
        relation = new EvalPlan(other->relation);
    else
        relation = nullptr;
    if (other->projection != nullptr)
        projection = new ColumnNames(*other->projection);
    else
        projection = nullptr;
    if (other->select_conjunction != nullptr)
        select_conjunction = new ValueDict(*other->select_conjunction);
    else
        select_conjunction = nullptr;
}

EvalPlan::~EvalPlan() {
    delete relation;
    delete projection;
    delete select_conjunction;
}


EvalPlan *EvalPlan::optimize() {
    return new EvalPlan(this);  // For now, we don't know how to do anything better
}

ValueDicts *EvalPlan::evaluate() {
    ValueDicts *ret = nullptr;
    if (this->type != ProjectAll && this->type != Project)
        throw DbRelationError("Invalid evaluation plan--not ending with a projection");

    EvalPipeline pipeline = this->relation->pipeline();
    DbRelation *temp_table = pipeline.first;
    Handles *handles = pipeline.second;
    if (this->type == ProjectAll)
        ret = temp_table->project(handles);
    else if (this->type == Project)
        ret = temp_table->project(handles, this->projection);
    delete handles;
    return ret;
}

EvalPipeline EvalPlan::pipeline() {
    // base cases
    if (this->type == TableScan)
        return EvalPipeline(&this->table, this->table.select());
    if (this->type == Select && this->relation->type == TableScan)
        return EvalPipeline(&this->relation->table, this->relation->table.select(this->select_conjunction));

    // recursive case
    if (this->type == Select) {
        EvalPipeline pipeline = this->relation->pipeline();
        DbRelation *temp_table = pipeline.first;
        Handles *handles = pipeline.second;
        EvalPipeline ret(temp_table, temp_table->select(handles, this->select_conjunction));
        delete handles;
        return ret;
    }

    throw DbRelationError("Not implemented: pipeline other than Select or TableScan");
}


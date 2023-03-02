/**
 * @file EvalPlan.h - Evaluation plans
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include "storage_engine.h"


typedef std::pair<DbRelation *, Handles *> EvalPipeline;

class EvalPlan {
public:
    enum PlanType {
        ProjectAll, Project, Select, TableScan
    };

    EvalPlan(PlanType type, EvalPlan *relation);  // use for ProjectAll, e.g., EvalPlan(EvalPlan::ProjectAll, table);
    EvalPlan(ColumnNames *projection, EvalPlan *relation); // use for Project
    EvalPlan(ValueDict *conjunction, EvalPlan *relation);  // use for Select
    EvalPlan(DbRelation &table);  // use for TableScan
    EvalPlan(const EvalPlan *other);  // use for copying
    virtual ~EvalPlan();

    // Attempt to get the best equivalent evaluation plan
    EvalPlan *optimize();

    // Evaluate the plan: evaluate gets values, pipeline gets handles
    ValueDicts *evaluate();

    EvalPipeline pipeline();

protected:

    PlanType type;
    EvalPlan *relation;  // for everything except TableScan
    ColumnNames *projection;  // for Project
    ValueDict *select_conjunction;  // for Select
    DbRelation &table;  // for TableScan
};


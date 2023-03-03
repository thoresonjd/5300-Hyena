# 5300-Hyena

DB Relation Manager project for CPSC5300 at Seattle U, Winter 2023, Project Hyena

The relation manager utilizes the [Hyrise SQL Parser for C++](https://github.com/hyrise/sql-parser) for parsing SQL statements and [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html) for managing database files.

## **Sprint Otoño**

Milestone 3

Milestone 4

## **Sprint Invierno**

### **Team**

Justin Thoreson & Jen-Chieh Lu

### **Milestone 5: Insert, Delete, Simple Queries**

Implementation of simple INSERT, DELETE, and SELECT statements.

Example queries:
```sql
INSERT INTO table (col_1, col_2, col_n) VALUES (1, 2, "three");
DELETE FROM table WHERE col_1 = 1;
SELECT * FROM table WHERE col_1 = 1 AND col_n = "three";
SELECT col_1, col_2 FROM table;
```

### **Compilation**

To compile, execute the [`Makefile`](./Makefile) via:
```
$ make
```

### **Usage**

To execute, run: 
```
$ ./sql5300 [ENV_DIR]
``` 
where `ENV_DIR` is the directory where the database environment resides.

SQL statements can be provided to the SQL shell when running. To terminate the SQL shell, run: 
```sql
SQL> quit
```

### **Testing**

To test the functionality of the relation manager, run:
```sql
SQL> test
```

### **Error & Memory Leak Checking**

If any issues arise, first try clearing out all the files within the database environment directory.

Checking for memory leaks can be done with [Valgrind](https://valgrind.org/). Valgrind error suppressions have been configured in the [`valgrind.supp`](./valgrind.supp) file. A target within the Makefile has been configured with relevant flags to execute Valgrind via running the command: 
```
$ make check
```
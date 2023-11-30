# Relational Algebra Engine

This project is a relational algebra engine developed as part of my CSC 1302 Honors Lab. While the parser and lexer were pre-existing components, significant coding efforts were dedicated to implementing core functionalities such as relation management, tuple operations, and relational algebra query execution.

## Project Overview

The engine operates on a database stored in a directory named "company," which contains relevant `.dat` files representing the database schema and data. The user can interact with the engine through the terminal, executing relational algebra queries on the specified database.

### 1. Database Management

The `Database` class facilitates the addition, deletion, and retrieval of relations. It initializes the database by reading schema and data files, populating the in-memory database with relation instances.

### 2. Relation Operations

The `Relation` class encompasses operations such as union, intersection, difference, projection, selection, renaming, and natural join. These operations are crucial for executing a wide range of relational algebra queries.

### 3. Tuple Handling

The `Tuple` class manages tuple operations, including cloning, concatenation, projection, and selection. These functionalities ensure the efficient manipulation of tuple data within relations.

## Example Queries

Here are some example queries that can be executed using the engine:
### Query 1

Retrieve the name and address of employees who work for the "Research" department.

```python
project[fname,lname,address](
  (rename[dname,dno,mgrssn,mgrstartdate](
      select[dname='Research'](department)) 
   join 
   employee
  )
);
```

### Query 2

For every project located in "Stafford," list the project number, the controlling department number, and the department manager's last name, address, and birth date.

```python
project[pnumber,dnum,lname,address,bdate](
  (
   (select[plocation='Stafford'](projects) 
    join 
    rename[dname,dnum,ssn,mgrstartdate](department)
   )
   join employee
  )
); 
```

Feel free to experiment with these queries to explore the capabilities of the relational algebra engine.

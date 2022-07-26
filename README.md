# Data modeling with Postgres

by Vicente Lizana

---

## Summary

At Sparkify, the Data Engineering team has created the Relational DB `sparkifydb`,
with the purpose of running analytical queries in order to extract information on the
users and how they use the platform, this way, informed decisions can be made on where
is worth to improve.

With this goal in mind, Dimensional Modeling was used to design this database and in
particular, the Star Schema was selected because of its popularity in data warehouses.
This denormalized schema allows to get the information needed in fast and simple queries.

## How to Run

To create (or recreate) the database:
```bash
python create_tables.py
```

To run the ETL, inserting into the database:
```bash
python etl.py
```

## Files on the project

- data: Directory containing datasets used for this project.
- etl.ipynb: Notebook for prototyping the functions used on the ETL.
- test.ipynb: Notebook with tests to ensure the quality of the ETL.
- sql_queries.py: Collection of SQL queries used throughout the project, including:
    - Creating and destroying tables.
    - Inserting into tables.
    - Querying for values.
- create_tables.py: Script for creating or recreating the tables used in the database.
- etl.py: Main script of the project, it Extracts, Transforms and Loads the data to the database.

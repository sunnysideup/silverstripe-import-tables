# tl;dr

Set up your credentials in your .env file like this:

```yml
# old database details to transfer data
SS_DATABASE_NAME_OLD_DB="old_db"
SS_DATABASE_SERVER_OLD_DB="localhost"
SS_DATABASE_USERNAME_OLD_DB="old_db_user"
SS_DATABASE_PASSWORD_OLD_DB="old_db_password"
```

Set up your schema in a yml file like this:

```yml
---
Name: app_import_tables
---
Sunnysideup\ImportTables\MoveTablesFromOldToNewDatabase:
  classes_to_move:
    - MyClassOne
    - MyClassTwo
  tables_to_move:
    - MyTableOne
    - MyTableTwo
    - MyOtherTable
  tables_to_skip:
    - MyTableThree
    - MyTableFour
  fields_to_skip:
    MyTableOne:
      - FieldOne
      - FieldTwo
  update_rather_than_replace: true
  always_update:
    - MyTableTwo
  always_replace:
    - MyOtherTable

```

then run the following task from the command line:

```bash
vendor/bin/sake dev/tasks/move-tables-from-old-to-new-database
```

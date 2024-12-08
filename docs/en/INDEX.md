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
  tables_to_skip:
    - MyTableThree
    - MyTableFour
  fields_to_skip:
    MyTableOne:
      - FieldOne
      - FieldTwo
```

then run the following task from the command line:

```bash
vendor/bin/sake dev/tasks/move-tables-from-old-to-new-database
```

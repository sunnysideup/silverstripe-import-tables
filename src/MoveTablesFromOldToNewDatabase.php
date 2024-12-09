<?php

namespace Sunnysideup\ImportTables;

use Exception;
use mysqli;
use SilverStripe\Core\Environment;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Dev\BuildTask;
use SilverStripe\ORM\DB;
use SilverStripe\Versioned\Versioned;

class MoveTablesFromOldToNewDatabase extends BuildTask
{
    protected $title = 'Move Tables From Old To New Database';

    protected $description = 'Move tables from old database to new database';

    protected $enabled = true;

    protected string $userNameOldDB;
    protected string $passwordOldDB;
    protected string $databaseNameOldDB;
    protected string $databaseHostOldDB;
    protected string $userNameNewDB;
    protected string $passwordNewDB;
    protected string $databaseNameNewDB;
    protected string $databaseHostNewDB;

    private static $classes_to_move = [];

    private static $tables_to_move = [];

    /**
     * @var array
     */
    private static $tables_to_skip = [];

    /**
     * ```php
     *         'MyTable' => [
     *            'Field1',
     *            'Field2',
     *        ],
     * ```
     * @var array
     */
    private static $field_to_skip = [];

    /**
     * ```php
     *        'OldClassName' => 'NewClassName',
     * ```
     * @var array
     */
    private static $class_names_to_fix = [

    ];

    private static $segment = 'move-tables-from-old-to-new-database';

    /**
     */
    public function run($request)
    {
        $this->getDbConfigs();
        $classes = $this->Config()->get('classes_to_move');
        foreach ($classes as $class) {
            $obj = Injector::inst()->get($class);
            $tableName = $class::config()->get('table_name');
            $this->moveTable($tableName);
            if ($obj->hasExtension(Versioned::class)) {
                $appendi = ['Live', 'Versions'];
                foreach ($appendi as $appendix) {
                    $versionedTableName = $tableName . '_' . $appendix;
                    $newExists = $this->doesTableExist(false, $versionedTableName);
                    if ($newExists) {
                        $this->moveTable($versionedTableName);
                    }
                }
            }
        }
        $tableNames = $this->Config()->get('tables_to_move');
        foreach ($tableNames as $tableName) {
            $this->moveTable($tableName);
        }
    }

    protected function getDbConfigs()
    {
        $this->databaseHostOldDB = Environment::getEnv('SS_DATABASE_SERVER_OLD_DB') ?: 'localhost';
        $this->userNameOldDB = Environment::getEnv('SS_DATABASE_USERNAME_OLD_DB');
        $this->passwordOldDB = Environment::getEnv('SS_DATABASE_PASSWORD_OLD_DB');
        $this->databaseNameOldDB = Environment::getEnv('SS_DATABASE_NAME_OLD_DB');
        if (! $this->databaseHostOldDB || ! $this->userNameOldDB || ! $this->passwordOldDB || ! $this->databaseNameOldDB) {
            throw new Exception(
                '
                Please provide all the required database configurations for the old database:
                    SS_DATABASE_SERVER_OLD_DB,
                    SS_DATABASE_USERNAME_OLD_DB,
                    SS_DATABASE_PASSWORD_OLD_DB,
                    SS_DATABASE_NAME_OLD_DB'
            );
        }
        $this->databaseHostNewDB = Environment::getEnv('SS_DATABASE_SERVER') ?: 'localhost';
        $this->userNameNewDB = Environment::getEnv('SS_DATABASE_USERNAME');
        $this->passwordNewDB = Environment::getEnv('SS_DATABASE_PASSWORD');
        $this->databaseNameNewDB = Environment::getEnv('SS_DATABASE_NAME');
        if (! $this->databaseHostNewDB || ! $this->userNameNewDB || ! $this->passwordNewDB || ! $this->databaseNameNewDB) {
            throw new Exception('
                Please provide all the required database configurations for the new database:
                SS_DATABASE_SERVER, SS_DATABASE_USERNAME, SS_DATABASE_PASSWORD, SS_DATABASE_NAME
            ');
        }
    }
    protected function moveTable(string $tableName): void
    {
        DB::alteration_message("Moving $tableName", 'created');

        if ($this->shouldSkipTable($tableName)) {
            return;
        }

        [$oldExists, $newExists] = $this->checkTableExistence($tableName);

        if ($oldExists && $newExists) {
            DB::alteration_message("... Truncating $tableName", 'error');
            $this->truncateTable($tableName);

            $commonFields = $this->findCommonFields($tableName);

            if (empty($commonFields)) {
                DB::alteration_message("... No matching fields found between the old and new table for: $tableName", 'error');
                return;
            }

            $rows = $this->fetchOldTableData($tableName, $commonFields);
            if (empty($rows)) {
                DB::alteration_message('... No data found in the old table.', 'notice');
                return;
            }

            $fieldTypes = $this->fetchFieldTypes($tableName);
            $allowedEnumValues = $this->getAllowedEnumValues($tableName);

            $this->insertRowsIntoNewTable($tableName, $commonFields, $rows, $fieldTypes, $allowedEnumValues);

            DB::alteration_message("... Moved rows successfully from $tableName", 'created');
        } elseif (!$oldExists) {
            throw new Exception("Table $tableName does not exist in the old database.");
        } elseif (!$newExists) {
            throw new Exception("Table $tableName does not exist in the new database.");
        }
    }

    protected function shouldSkipTable(string $tableName): bool
    {
        $tablesToSkip = $this->Config()->get('tables_to_skip');
        if (in_array($tableName, $tablesToSkip)) {
            DB::alteration_message("... Skipping $tableName", 'notice');
            return true;
        }
        return false;
    }

    protected function checkTableExistence(string $tableName): array
    {
        $oldExists = $this->doesTableExist(true, $tableName);
        $newExists = $this->doesTableExist(false, $tableName);
        return [$oldExists, $newExists];
    }

    protected function findCommonFields(string $tableName): array
    {
        $fieldsToSkip = $this->Config()->get('field_to_skip');
        $oldFields = $this->getTableFields(true, $tableName);
        $newFields = $this->getTableFields(false, $tableName);
        $commonFields = array_intersect($oldFields, $newFields);

        if (isset($fieldsToSkip[$tableName])) {
            $commonFields = array_diff($commonFields, $fieldsToSkip[$tableName]);
        }

        return $commonFields;
    }

    protected function fetchOldTableData(string $tableName, array $commonFields): array
    {
        [$host, $username, $password, $database] = $this->getDbConfig(true);
        $oldDBConnection = new mysqli($host, $username, $password, $database);
        if ($oldDBConnection->connect_error) {
            throw new Exception('Old DB Connection failed: ' . $oldDBConnection->connect_error);
        }

        $fieldList = '`' . implode('`, `', $commonFields) . '`';
        $query = "SELECT $fieldList FROM `$tableName`";
        $result = $oldDBConnection->query($query);
        if (!$result) {
            $oldDBConnection->close();
            throw new Exception('Error fetching data from old table: ' . $oldDBConnection->error);
        }

        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $result->free();
        $oldDBConnection->close();

        return $rows;
    }

    protected function fetchFieldTypes(string $tableName): array
    {
        [$host, $username, $password, $database] = $this->getDbConfig(true);
        $oldDBConnection = new mysqli($host, $username, $password, $database);

        $query = "SHOW COLUMNS FROM `$tableName`";
        $result = $oldDBConnection->query($query);
        if (!$result) {
            $oldDBConnection->close();
            throw new Exception('Error fetching field types: ' . $oldDBConnection->error);
        }

        $fieldTypes = [];
        while ($field = $result->fetch_assoc()) {
            $fieldTypes[$field['Field']] = $field['Type'];
        }
        $result->free();
        $oldDBConnection->close();

        return $fieldTypes;
    }

    protected function getAllowedEnumValues(string $tableName): array
    {
        [$host, $username, $password, $database] = $this->getDbConfig(false);
        $newDBConnection = new mysqli($host, $username, $password, $database);

        $query = "SHOW COLUMNS FROM `$tableName` WHERE Field = 'ClassName'";
        $result = $newDBConnection->query($query);
        if (!$result) {
            $newDBConnection->close();
            throw new Exception('Error fetching enum values: ' . $newDBConnection->error);
        }

        $enumRow = $result->fetch_assoc();
        $result->free();

        if (isset($enumRow['Type']) && preg_match("/^enum\((.*)\)$/", $enumRow['Type'], $matches)) {
            return array_map(fn ($value) => trim($value, "'"), explode(',', $matches[1]));
        }

        return [];
    }
    protected function insertRowsIntoNewTable(
        string $tableName,
        array $commonFields,
        array $rows,
        array $fieldTypes,
        array $allowedEnumValues
    ): void {
        $classesToFix = $this->Config()->get('class_names_to_fix');
        [$host, $username, $password, $database] = $this->getDbConfig(false);
        $newDBConnection = new mysqli($host, $username, $password, $database);
        if ($newDBConnection->connect_error) {
            throw new Exception('New DB Connection failed: ' . $newDBConnection->connect_error);
        }

        $fieldList = '`' . implode('`, `', $commonFields) . '`';
        $newDBConnection->begin_transaction();

        try {
            foreach ($rows as $row) {
                $placeholders = implode(', ', array_fill(0, count($commonFields), '?'));
                $insertQuery = "INSERT INTO `$tableName` ($fieldList) VALUES ($placeholders)";
                $stmt = $newDBConnection->prepare($insertQuery);
                if (!$stmt) {
                    throw new Exception('Error preparing statement: ' . $newDBConnection->error);
                }

                $types = '';
                $values = [];
                foreach ($commonFields as $field) {
                    $value = $row[$field];

                    // Validate and fix the value for ClassName
                    if ($field === 'ClassName') {
                        $value = $classesToFix[$value] ?? $value;
                        if (! $value) {
                            $value = $allowedEnumValues[0] ?? null;
                        }
                        $escapedValue = $value;
                        $escapedValue = $classesToFix[$escapedValue] ?? $escapedValue;
                        if (!in_array($escapedValue, $allowedEnumValues, true)) {
                            $escapedValue = addslashes($value);
                        } else {
                            $value = stripslashes($value);
                        }
                        if (!in_array($escapedValue, $allowedEnumValues, true)) {
                            error_log("Invalid value for 'ClassName': $escapedValue. Allowed values: " . implode(', ', $allowedEnumValues));
                            $value = $allowedEnumValues[0] ?? null; // Use the first allowed value as a fallback
                            if ($value === null) {
                                throw new Exception("No valid enum value found for 'ClassName'.");
                            }
                        }
                    }

                    $types .= $this->mapFieldTypeToBindType($fieldTypes[$field] ?? '');
                    $values[] = $value;
                }

                $stmt->bind_param($types, ...$values);
                if (!$stmt->execute()) {
                    throw new Exception('Error executing statement: ' . $stmt->error);
                }
                $stmt->close();
            }

            $newDBConnection->commit();
        } catch (Exception $e) {
            $newDBConnection->rollback();
            throw $e;
        } finally {
            $newDBConnection->close();
        }
    }



    public function checkSuccess($tableName)
    {
        [$hostOldDB, $usernameOldDB, $passwordOldDB, $databaseOldDB] = $this->getDbConfig(true);
        [$hostNewDB, $usernameNewDB, $passwordNewDB, $databaseNewDB] = $this->getDbConfig(false);

        // Connect to the new database
        $newDBConnection = new mysqli($hostNewDB, $usernameNewDB, $passwordNewDB, $databaseNewDB);
        if ($newDBConnection->connect_error) {
            throw new Exception('New DB Connection failed: ' . $newDBConnection->connect_error);
        }

        // Fetch row count from the old table
        $oldRowCountQuery = "SELECT COUNT(*) AS rowCount FROM `$tableName`";
        $oldDBConnection = new mysqli($hostOldDB, $usernameOldDB, $passwordOldDB, $databaseOldDB);
        if ($oldDBConnection->connect_error) {
            throw new Exception('Old DB Connection failed: ' . $oldDBConnection->connect_error);
        }
        $oldRowCountResult = $oldDBConnection->query($oldRowCountQuery);
        if (!$oldRowCountResult) {
            throw new Exception('Error fetching row count from old table: ' . $oldDBConnection->error);
        }
        $oldRowCount = $oldRowCountResult->fetch_assoc()['rowCount'];
        $oldRowCountResult->free();
        $oldDBConnection->close();

        // Fetch row count from the new table
        $newRowCountQuery = "SELECT COUNT(*) AS rowCount FROM `$tableName`";
        $newRowCountResult = $newDBConnection->query($newRowCountQuery);
        if (!$newRowCountResult) {
            throw new Exception('Error fetching row count from new table: ' . $newDBConnection->error);
        }
        $newRowCount = $newRowCountResult->fetch_assoc()['rowCount'];
        $newRowCountResult->free();

        // Compare the row counts
        if ($oldRowCount !== $newRowCount) {
            throw new Exception("Row count mismatch for `$tableName`. Old table: $oldRowCount rows, New table: $newRowCount rows.");
        }

        DB::alteration_message(
            "Row count verification passed for `$tableName`. Old table: $oldRowCount rows, New table: $newRowCount rows.",
            'created'
        );
        $this->checkSuccess($tableName);

    }



    // Helper to map MySQL field types to bind_param types
    public function mapFieldTypeToBindType(string $fieldType): string
    {
        if (stripos($fieldType, 'int') !== false) {
            return 'i'; // Integer
        } elseif (stripos($fieldType, 'float') !== false || stripos($fieldType, 'double') !== false || stripos($fieldType, 'decimal') !== false) {
            return 'd'; // Double
        } elseif (stripos($fieldType, 'blob') !== false || stripos($fieldType, 'binary') !== false) {
            return 'b'; // Blob
        } else {
            return 's'; // String
        }
    }

    protected function getTableFields(bool $isOldDB, string $tableName): array
    {
        [$host, $username, $password, $database] = $this->getDbConfig($isOldDB);
        // Create a new MySQLi connection
        $mysqli = new mysqli($host, $username, $password, $database);

        // Check the connection
        if ($mysqli->connect_error) {
            throw new Exception('Connection failed: ' . $mysqli->connect_error);
        }

        // Prepare the query to describe the table
        $query = 'DESCRIBE `' . $mysqli->real_escape_string($tableName).'`';
        $result = $mysqli->query($query);

        if (!$result) {
            throw new Exception('Query failed: ' . $mysqli->error);
        }

        // Fetch field names
        $fields = [];
        while ($row = $result->fetch_assoc()) {
            $fields[] = $row['Field'];
        }

        // Free result and close connection
        $result->free();
        $mysqli->close();

        return $fields;
    }

    protected function truncateTable(string $tableName): bool
    {
        DB::query('TRUNCATE TABLE "' . $tableName.'"');

        return true;
    }

    public function doesTableExist(bool $isOldDB, string $tableName): bool
    {
        // Connect to the database
        [$host, $username, $password, $database] = $this->getDbConfig($isOldDB);
        $mysqli = new mysqli($host, $username, $password, $database);

        // Check for connection errors
        if ($mysqli->connect_error) {
            die('Connection failed: ' . $mysqli->connect_error);
        }

        // Prepare the query to check if the table exists
        $query = 'SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_SCHEMA = ?
                AND TABLE_NAME = ?';

        $stmt = $mysqli->prepare($query);

        if (!$stmt) {
            // Handle query preparation error
            die('Query preparation failed: ' . $mysqli->error);
        }

        // Bind parameters
        $stmt->bind_param('ss', $database, $tableName);

        // Execute the query
        $stmt->execute();

        // Store the result
        $stmt->store_result();

        // Check if the table exists
        $exists = $stmt->num_rows > 0;

        // Close the statement and connection
        $stmt->close();
        $mysqli->close();

        return $exists;
    }


    protected function getDbConfig(bool $isOldDB)
    {
        if ($isOldDB) {
            return [$this->databaseHostOldDB, $this->userNameOldDB, $this->passwordOldDB, $this->databaseNameOldDB];
        } else {
            return [$this->databaseHostNewDB, $this->userNameNewDB, $this->passwordNewDB, $this->databaseNameNewDB];
        }
    }
}

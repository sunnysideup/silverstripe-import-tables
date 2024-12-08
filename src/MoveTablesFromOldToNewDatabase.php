<?php

namespace Sunnysideup\ImportTables;

use Exception;
use mysqli;
use SilverStripe\CMS\Model\SiteTree;
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

    private static $classes_to_move = [
    ];

    private static $tables_to_move = [
    ];

    private static $field_to_skip = [
        'MyTable' => [
            'Field1',
            'Field2',
        ],
    ];


    /**
     */
    public function run($request)
    {
        $this->getDbConfigs();
        $classes = $this->Config()->get('classes_to_move');
        foreach ($classes as $class) {
            $obj = Injector::inst()->get($class);
            $tableName = $class::config()->get('table_name');
            $this->truncateTable($tableName);
            $this->moveTable($tableName);
            if ($obj->hasExtension(Versioned::class)) {
                $appendi = ['Live', 'Versions'];
                foreach ($appendi as $appendix) {
                    $versionedTableName = $tableName . '_' . $appendix;
                    $this->truncateTable($versionedTableName);
                    $this->moveTable($versionedTableName);
                }
            }
        }
        $classes = $this->Config()->get('tables_to_move');
        foreach ($classes as $class) {
            $this->truncateTable($tableName);
            $this->moveTable($tableName);
        }
    }

    protected function getDbConfigs()
    {
        $this->databaseHostOldDB = Environment::getEnv('SS_DATABASE_HOST_OLD_DB') ?: 'localhost';
        $this->userNameOldDB = Environment::getEnv('SS_DATABASE_USERNAME_OLD_DB');
        $this->passwordOldDB = Environment::getEnv('SS_DATABASE_PASSWORD_OLD_DB');
        $this->databaseNameOldDB = Environment::getEnv('SS_DATABASE_NAME_OLD_DB');
        if (! $this->databaseHostOldDB || ! $this->userNameOldDB || ! $this->passwordOldDB || ! $this->databaseNameOldDB) {
            throw new Exception('Please provide all the required database configurations for the old database: SS_DATABASE_HOST_OLD_DB, SS_DATABASE_USERNAME_OLD_DB, SS_DATABASE_PASSWORD_OLD_DB, SS_DATABASE_NAME_OLD_DB');
        }
        $this->databaseHostNewDB = Environment::getEnv('SS_DATABASE_HOST_') ?: 'localhost';
        $this->userNameNewDB = Environment::getEnv('SS_DATABASE_USERNAME_');
        $this->passwordNewDB = Environment::getEnv('SS_DATABASE_PASSWORD_');
        $this->databaseNameNewDB = Environment::getEnv('SS_DATABASE_NAME_');
        if (! $this->databaseHostNewDB || ! $this->userNameNewDB || ! $this->passwordNewDB || ! $this->databaseNameNewDB) {
            throw new Exception('Please provide all the required database configurations for the new database: SS_DATABASE_HOST_, SS_DATABASE_USERNAME_, SS_DATABASE_PASSWORD_, SS_DATABASE_NAME_');
        }
    }


    protected function moveTable(string $tableName)
    {
        $fieldsToSkip = $this->Config()->get('field_to_skip');
        $oldFields = $this->getTableFields(true, $tableName);
        $newFields = $this->getTableFields(false, $tableName);
        [$hostOldDB, $usernameOldDB, $passwordOldDB, $databaseOldDB] = $this->getDbConfig(true);
        [$hostNewDB, $usernameNewDB, $passwordNewDB, $databaseNewDB] = $this->getDbConfig(false);

        // Find common fields between old and new table
        $commonFields = array_intersect($oldFields, $newFields);

        $commonFields = array_intersect($oldFields, $newFields);
        if (isset($fieldsToSkip[$tableName])) {
            $commonFields = array_diff($commonFields, $fieldsToSkip[$tableName]);
        }

        if (empty($commonFields)) {
            throw new Exception('No matching fields found between the old and new table.');
        }

        // Prepare the field list
        $fieldList = implode(', ', $commonFields);

        // Connect to the old database
        $oldDBConnection = new mysqli($hostOldDB, $usernameOldDB, $passwordOldDB, $databaseOldDB);
        if ($oldDBConnection->connect_error) {
            throw new Exception('Old DB Connection failed: ' . $oldDBConnection->connect_error);
        }

        // Fetch data from the old table
        $query = "SELECT $fieldList FROM $tableName";
        $result = $oldDBConnection->query($query);
        if (!$result) {
            $oldDBConnection->close();
            throw new Exception('Error fetching data from old table: ' . $oldDBConnection->error);
        }

        // Prepare data for insertion
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $result->free();
        $oldDBConnection->close();

        if (empty($rows)) {
            throw new Exception('No data found in the old table.');
        }

        // Connect to the new database
        $newDBConnection = new mysqli($hostNewDB, $usernameNewDB, $passwordNewDB, $databaseNewDB);
        if ($newDBConnection->connect_error) {
            throw new Exception('New DB Connection failed: ' . $newDBConnection->connect_error);
        }

        // Determine field types from the old database
        $fieldTypesQuery = "SHOW COLUMNS FROM $tableName";
        $fieldTypesResult = $oldDBConnection->query($fieldTypesQuery);
        if (!$fieldTypesResult) {
            $oldDBConnection->close();
            throw new Exception('Error fetching field types: ' . $oldDBConnection->error);
        }

        $fieldTypes = [];
        while ($field = $fieldTypesResult->fetch_assoc()) {
            $fieldTypes[$field['Field']] = $field['Type'];
        }
        $fieldTypesResult->free();



        // Insert data into the new table
        $insertedRowsCount = 0;
        $newDBConnection->begin_transaction();
        try {
            foreach ($rows as $row) {
                $placeholders = implode(', ', array_fill(0, count($commonFields), '?'));
                $insertQuery = "INSERT INTO $tableName ($fieldList) VALUES ($placeholders)";
                $stmt = $newDBConnection->prepare($insertQuery);
                if (!$stmt) {
                    throw new Exception('Error preparing statement: ' . $newDBConnection->error);
                }

                // Map types and bind values dynamically
                $types = '';
                $values = [];
                foreach ($commonFields as $field) {
                    $types .= $this->mapFieldTypeToBindType($fieldTypes[$field]);
                    $values[] = $row[$field];
                }

                $stmt->bind_param($types, ...$values);
                if (!$stmt->execute()) {
                    throw new Exception('Error executing statement: ' . $stmt->error);
                }

                $insertedRowsCount++;
                $stmt->close();
            }

            $newDBConnection->commit();
        } catch (Exception $e) {
            $newDBConnection->rollback();
            $newDBConnection->close();
            throw $e;
        }

        $newDBConnection->close();
        DB::alteration_message("Moved $insertedRowsCount rows from $tableName", 'created');
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
        $query = 'DESCRIBE ' . $mysqli->real_escape_string($tableName);
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
        DB::query('TRUNCATE TABLE ' . $tableName);

        return true;
    }

    protected function getDbConfig(bool $isOldDB)
    {
        if ($isOldDB) {
            $host = $this->databaseHostOldDB;
            $username = $this->userNameOldDB;
            $password = $this->passwordOldDB;
            $database = $this->databaseNameOldDB;
        } else {
            $host = $this->databaseHostNewDB;
            $username = $this->userNameNewDB;
            $password = $this->passwordNewDB;
            $database = $this->databaseNameNewDB;
        }
        return [$host, $username, $password, $database];
    }
}

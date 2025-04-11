# Database structure comparer.


## Installing required software
It is expected that you have python3 installed.
```bash
$ python3 -m venv venv

$ source venv/bin/activate

(venv) $ pip install -r requirements.txt
```

## Database Schema Analyzer and Configuration Generator

This script is a database schema analyzer and configuration generator that:

1. Extracts and maps SQL schema definitions to standardized data types
2. Parses SQL table structures, including columns and their properties
3. Handles SQL type conversions using a predefined TYPE_MAP in order to generate `json` files compatible with YeAPF2 documents constraints definitions file.
4. Processes SQL definition blocks with proper handling of quotes and parentheses
5. Generates structured configuration files for database schemas
6. Supports various SQL data types including varchar, int, datetime, etc.
7. Outputs schema information in two standardized format: SQL and YeAPF2 JSON documents schemas.

Expected configuration structure:
```bash
/[host]/
  └── [database]/
      └── connection.json
```
Used for *source* database schema management and configuration generation.

### Usage example

```bash
$ source venv/bin/activate

(venv) $ python get-current-config.py --host 192.168.2.15 --database example_db
```

## Configuration example:
```json
{
    "host": "192.168.2.15",
    "database": "example_db",
    "user": "root",
    "password": "password",
    "port": 3306
}
```

### Advantages of using this script

You don't need to be a SQL expert to use this script. It simplifies the process of generating database schema configurations.

You can use this script to generate database schema configurations for your projects.

You don't need to be connected to the target database to use this script.


## Schema Updates Generator

This script automates the process of generating and managing database schema updates. It provides functionality to:

1. Compare and synchronize database schema structures between JSON definitions and actual MySQL databases
2. Track schema changes using timestamped folders
3. Generate SQL statements for schema modifications (ADD, MODIFY, DROP columns)
4. Handle database connections using configuration files
5. Support multiple database hosts and schemas

### Key Features

- Loads database connection configurations from JSON file (`connection.json` in each host/database folder)
- Detects the latest schema version using timestamped folders (user can explicity specify the version)
- Compares table structures between JSON definitions and live databases
- Generates SQL statements for schema updates
- Normalizes SQL data types for consistent comparisons
- Supports multiple database environments through host/database folder structure

### Usage

The script expects a specific folder structure containing:
- Connection configurations in `connection.json`
- Table definitions in JSON format
- Timestamped folders for version tracking

Required folder structure:
```bash
├── [host]
│   └── [database]
│       ├── connection.json
│       └── [timestamp]
│           └── *.json
```

Once you have the `connection.json` file in the host/database folder and you already user `get-current-config.py`, you can run the script to generate the SQL statements.

```bash
$ source venv/bin/activate

(venv) $ python generate-schema-updates.py 192.168.2.15:example_db 10.0.1.78:target_example_db
```

This command will generate SQL statements for the specified target host and database. Pay attention that even declaring that the source connection is 192.168.2.15:example_db, the script will use the connection.json file in the folder 192.168.2.15:example_db to generate the SQL statements. That means, again, that you don't be to be connected to the source database to generate the SQL statements.

This is exrtemely useful if you want to generate the SQL statements for a target database that you don't have access to. Or if you cannot have access to the source database, for example, if you are using a cloud database or the two databases are not connected to the same network.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
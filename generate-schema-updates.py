import os
import json
import mysql.connector
from datetime import datetime
from glob import glob
from pathlib import Path

"""
[ in source system connection.json -> get-current-schema.py -> schemas.json ]
schemas.json -> generate-schema-updates.py -> update-schema.sql

This script generates SQL schema updates by comparing target JSON schema definitions against the actual database structure.
It uses MySQL connector to fetch current table structures and compares them with JSON schema files (likely produced by get-current-schema.py).
The script identifies required additions, modifications, and drops of columns by comparing target schemas with actual database state.

THIS SCRIPT DOES NOT MODIFY THE DATABASE. You need to run the update-schema.sql manually.

(C) 2025 - Esteban D.Dortta - MIT License
"""


def get_latest_timestamp(base_path):
    print(f"Getting latest timestamp from {base_path}")

    timestamps = sorted(Path(base_path).glob("[0-9][0-9][0-9][0-9]-*-*-*"), reverse=True)
    return timestamps[0].name if timestamps else None

def load_connection_config(folder, host, database):
    path = Path(f"{folder}/{host}/{database}/connection.json")
    if not path.exists():
        raise FileNotFoundError(f"Connection file not found: {path}")
    with open(path) as f:
        return json.load(f)

def load_json_tables(base_path):
    return {
        Path(f).stem: json.load(open(f))
        for f in glob(f"{base_path}/*.json") if not f.endswith("indexes.json")
    }

def fetch_table_structure(cursor, dbname):
    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (dbname,))
    table_names = [row[0] for row in cursor.fetchall()]
    tables = {}
    for table in table_names:
        cursor.execute(f"SHOW FULL COLUMNS FROM `{table}`")
        tables[table] = cursor.fetchall()
    return tables

def normalize_type(sql_type):
    sql_type = sql_type.lower()
    if '(' in sql_type:
        base = sql_type.split('(')[0]
    else:
        base = sql_type
    return base.strip()

def compare_and_generate_sql(target_json, actual_fields):
    adds, modifies, drops = [], [], []

    actual_col_map = {row[0]: row for row in actual_fields}
    actual_cols = set(actual_col_map.keys())
    expected_cols = set(target_json.keys())

    for col, spec in target_json.items():
        col_type = spec["type"].upper()
        length = spec.get("length")
        mysql_type = {
            "STRING": f"varchar({length})" if length else "text",
            "INTEGER": "int",
            "BOOLEAN": "tinyint(1)",
            "DATETIME": "datetime",
        }.get(col_type, "text")

        nullable = "NULL" if spec.get("acceptNULL", True) else "NOT NULL"
        column_def = f"`{col}` {mysql_type} {nullable}"

        if col not in actual_cols:
            adds.append(f"ADD COLUMN {column_def}")
        else:
            actual = actual_col_map[col]
            actual_type = normalize_type(actual[1])
            expected_type = normalize_type(mysql_type)

            actual_nullable = actual[3] == "YES"
            expected_nullable = spec.get("acceptNULL", True)

            if actual_type != expected_type or actual_nullable != expected_nullable:
                modifies.append(f"MODIFY COLUMN {column_def}")

    for col in actual_cols - expected_cols:
        drops.append(f"DROP COLUMN `{col}`")

    return adds, modifies, drops

def main(src_host, src_database, target_host, target_database, timestamp=None):
    conn_cfg = load_connection_config('updates', target_host, target_database)
    base_path = f"dump/{src_host}/{src_database}"
    timestamp = timestamp or get_latest_timestamp(base_path)
    if not timestamp:
        raise Exception("No timestamp folder found.")    
    dump_path = f"{base_path}/{timestamp}"


    output_path = f"updates/{conn_cfg['host']}/{conn_cfg['database']}/{timestamp}"
    os.makedirs(output_path, exist_ok=True)

    print(f"📡 Connecting to database `{conn_cfg['database']}` at {conn_cfg['host']}:{conn_cfg.get('port',3306)}")
    conn = mysql.connector.connect(**conn_cfg)
    cursor = conn.cursor()

    actual_struct = fetch_table_structure(cursor, conn_cfg['database'])
    expected_tables = load_json_tables(dump_path)

    for table, expected in expected_tables.items():
        table_filename = f"{output_path}/{table}-structure.sql"
        if os.path.exists(table_filename):
            os.remove(table_filename)

        f = open(table_filename, "w")

        if table not in actual_struct:
            print(f"Table `{table}` does not exist, creating it.")
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS `{table}` (
                dummy_column BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
            )""")
            conn.commit()
            with open(f"{dump_path}/{table}.sql") as fd:
                f.write(f"// Table {table} created with dummy_column\n")    

        adds, modifies, drops = compare_and_generate_sql(expected, actual_struct.get(table, []))

        if adds or modifies:
            for cmd in adds + modifies:
                f.write(f"ALTER TABLE `{table}` {cmd};\n")

        if drops:
            for cmd in drops:
                f.write(f"ALTER TABLE `{table}` {cmd};\n")

        print(f"{table}: {len(adds)} adds, {len(modifies)} modifies, {len(drops)} drops")

    cursor.close()
    conn.close()
    print(f"\nAll update scripts written to {output_path}/")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate schema update SQL scripts from JSON definitions.")
    parser.add_argument("source", help="The source host and database in the format host:database (used for config)")
    parser.add_argument("target", help="The target host and database in the format host:database (used for config)")
    parser.add_argument("--timestamp", help="Optional timestamp folder, otherwise most recent is used")

    args = parser.parse_args()
    src_host, src_database = args.source.split(":")
    target_host, target_database = args.target.split(":")
    main(src_host, src_database, target_host, target_database, args.timestamp)


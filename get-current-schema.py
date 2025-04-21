import os
import subprocess
import re
import sqlparse
import json
from collections import defaultdict
from datetime import datetime

"""
connection.json -> get-current-schema.py -> schemas.json 
[ in the target system, schemas.json -> generate-schema-updates.py -> update-schema.sql ]

This script extracts and maps the current database schema from SQL to a standardized format.
It parses SQL table definitions and converts SQL types to standardized types using TYPE_MAP.
TYPE_MAP is compatible with YeAPF2's database types.
The script handles SQL parsing including quoted strings and nested parentheses for complex definitions.
The script also handles primary keys and indexes but separately from the table definition.
In such way, we can later update the target schema to match the current one.

THIS SCRIT DOES NOT MODIFY THE DATABASE. It just generates the JSON files that will be 
used by generate-schema-updates.py to generate the update-schema.sql in the target system.

(C) 2025 - Esteban D.Dortta - MIT License
"""

OUTDIR = None
STRUCTURE_FILE = None
INDEXES_FILE = None
DB = None

TYPE_MAP = {
    "varchar": "STRING",
    "char": "STRING",
    "int": "INTEGER",
    "bigint": "INTEGER",
    "datetime": "DATETIME",
    "timestamp": "DATETIME",
    "text": "STRING",
    "tinyint": "INTEGER",
    "boolean": "BOOLEAN"
}


def map_sql_type(sql_type):
    match = re.match(r"(\w+)(?:\((\d+)\))?", sql_type)
    if not match:
        return {"type": "STRING"}
    base, length = match.groups()
    base = base.lower()
    result = {"type": TYPE_MAP.get(base, "STRING")}
    if length:
        result["length"] = int(length)
    return result


import re

def split_sql_definitions(definition_block):
    parts = []
    buffer = ""
    parens = 0
    in_quotes = False
    quote_char = ""

    i = 0
    while i < len(definition_block):
        char = definition_block[i]

        # Handle quote start/end
        if char in ("'", '"'):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif quote_char == char:
                # Check for escaped quote (e.g., 'It''s something')
                if i + 1 < len(definition_block) and definition_block[i + 1] == quote_char:
                    buffer += char  # Add the first quote
                    i += 1  # Skip escaped quote
                else:
                    in_quotes = False
                    quote_char = ""

        if not in_quotes:
            if char == '(':
                parens += 1
            elif char == ')':
                parens -= 1
            if char == ',' and parens == 0:
                parts.append(buffer.strip())
                buffer = ""
                i += 1
                continue

        buffer += char
        i += 1

    if buffer.strip():
        parts.append(buffer.strip())

    return parts

def split_create_table_statement(create_sql):
    commands = []
    auxiliar = []

    # Match even if missing semicolon or final ENGINE clause
    table_pattern = r"CREATE TABLE\s+`([^`]+)`\s*\((.*)"
    match = re.search(table_pattern, create_sql, re.DOTALL | re.IGNORECASE)

    if not match:
        return [], []

    table_name = match.group(1)
    column_block = match.group(2).strip()

    definitions = split_sql_definitions(column_block)

    column_defs = []
    index_defs = []
    for line in definitions:
        line_upper = line.upper()
        if line_upper.startswith(("PRIMARY KEY", "KEY", "UNIQUE KEY", "CONSTRAINT", "FOREIGN KEY")):
            index_defs.append(line)
        else:
            column_defs.append(line)

    # Compose CREATE TABLE (only columns)
    create_stmt = f"CREATE TABLE `{table_name}` (\n    " + ",\n    ".join(column_defs) + "\n);"
    commands.append(create_stmt)

    # Compose ALTER TABLE for indexes/constraints
    for index in index_defs:
        alter_stmt = f"ALTER TABLE `{table_name}` ADD {index};"
        auxiliar.append(alter_stmt)

    return commands, auxiliar


def run_mysqldump():
    print(f"Dumping database into {OUTDIR}...")
    os.makedirs(OUTDIR, exist_ok=True)
    print("Dumping table structure...")
    structure_cmd = [
        "mysqldump",
        f"-h{DB['host']}",
        f"-P{DB['port']}",
        f"-u{DB['user']}",
        f"-p{DB['password']}",
        DB["database"],
        "--no-data", "--skip-comments", "--compact"
    ]
    with open(STRUCTURE_FILE, "w") as f:
        subprocess.run(structure_cmd, stdout=f, check=True)
        f.close()


    database_identifier = f"`{DB['database']}`"
    database_identifier=""


    print("Dumping indexes...")
    indexes = []
    tables = []
    current_line = ''
    with open(STRUCTURE_FILE, "r") as f:
        for line in f:            
            if line.endswith(";\n"):                
                final_line = current_line.replace("\n"," ")
                if (final_line.startswith("CREATE TABLE ")):
                    tables.append(final_line)
                current_line = ''
            else:
                current_line += line.strip()

    if (current_line != ''):
        final_line = current_line.replace("\n"," ")
        if (final_line.startswith("CREATE TABLE ")):
            tables.append(final_line)

    for line in tables:
        cmds = split_create_table_statement(line)
        indexes.append(cmds[1])

    # with open(STRUCTURE_FILE, "w") as f:
    #     for line in tables:
    #         cmds = split_create_table_statement(line)
    #         indexes.append(cmds[1])
    #         f.write(cmds[0][0]+"\n\n")

   
    with open(INDEXES_FILE, "w") as f:
        for index in indexes:
            for ndx in index:
                f.write(ndx + "\n")
        f.close()


def parse_structure(sql_text):
    tables = defaultdict(dict)
    statements = sqlparse.split(sql_text)

    for stmt in statements:
        create_match = re.search(r"CREATE TABLE\s+`?(\w+)`?\s*\((.*)\)[^)]*;", stmt, re.S | re.I)
        if not create_match:
            continue

        table_name, body = create_match.groups()

        
        with open(f"{OUTDIR}/{table_name}.sql", "w") as f_sql:
            f_sql.write(stmt.strip() + "\n")

        lines = sqlparse.format(body, strip_comments=True).splitlines()
        current = ""
        columns = []

        for line in lines:
            line = line.strip()
            if not line:
                continue
            current += " " + line
            if line.endswith(",") or line.endswith(")") or "PRIMARY KEY" in line or "KEY" in line:
                columns.append(current.strip().rstrip(","))
                current = ""

        for col_line in columns:
            if col_line.upper().startswith(("PRIMARY KEY", "UNIQUE KEY", "KEY")):
                continue  
            col_match = re.match(r'`(\w+)`\s+([a-zA-Z0-9_]+(\([^\)]+\))?)(.*)', col_line)
            if not col_match:
                continue
            col_name, col_type, _, rest = col_match.groups()
            info = map_sql_type(col_type)
            info["acceptNULL"] = "NOT NULL" not in rest.upper()
            if "NOT NULL" in rest.upper():
                info["required"] = True
            tables[table_name][col_name] = info

        
        pk_match = re.findall(r'PRIMARY KEY\s*\((.*?)\)', body)
        for group in pk_match:
            keys = [k.strip("` ") for k in group.split(",")]
            for k in keys:
                if k in tables[table_name]:
                    tables[table_name][k]["primary"] = True

    return tables


def parse_indexes(sql_text):
    index_map = defaultdict(list)
    statements = sqlparse.split(sql_text)

    for stmt in statements:
        match = re.search(r'ALTER TABLE `?(\w+)`?\s+(ADD .+)', stmt, re.S | re.I)
        if not match:
            continue
        table_name, actions = match.groups()
        for action in actions.split("ADD "):
            action = action.strip().strip(",;")
            if not action:
                continue
            index_info = {"raw": action}
            idx_match = re.match(r'(UNIQUE\s+)?KEY `?(\w+)`? \((.+?)\)', action)
            if idx_match:
                is_unique, name, cols = idx_match.groups()
                index_info["name"] = name
                index_info["columns"] = [c.strip("` ") for c in cols.split(",")]
                index_info["unique"] = bool(is_unique)
            index_map[table_name].append(index_info)

    return index_map


def write_jsons(structure, indexes):
    for table, fields in structure.items():
        with open(f"{OUTDIR}/{table}.json", "w") as f:
            json.dump(fields, f, indent=2)

    with open(f"{OUTDIR}/all-tables-indexes.json", "w") as f:
        json.dump(indexes, f, indent=2)


def main(host, database):
    global DB, OUTDIR, STRUCTURE_FILE, INDEXES_FILE

    DUMP_DIR = os.path.join("dump", host, database)
    conn_file = os.path.join(DUMP_DIR, "connection.json")
    if os.path.exists(conn_file):
        with open(conn_file) as f:
            DB = json.load(f)
    
    
        TS = datetime.now().strftime("%Y-%m-%d-%H")
        OUTDIR = os.path.join(DUMP_DIR, TS)
        STRUCTURE_FILE = os.path.join(OUTDIR, "all-tables-structure.sql")
        INDEXES_FILE = os.path.join(OUTDIR, "all-tables-indexes.sql")

        print(f"Dumping {host}:{database} to {OUTDIR}")

        run_mysqldump()

        with open(STRUCTURE_FILE) as f:
            structure_sql = f.read()
        with open(INDEXES_FILE) as f:
            indexes_sql = f.read()

        print("Parsing structure...")
        structure = parse_structure(structure_sql)
        print("Parsing indexes...")
        indexes = parse_indexes(indexes_sql)

        print("Generating output files...")
        write_jsons(structure, indexes)

        print(f"Done! All files in {OUTDIR}/")
    else:
        print(f"No connection file found for {host}:{database}")
        print(f"You need to have a file called 'connection.json' at {DUMP_DIR}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Get current database structure.")
    parser.add_argument("--host", help="The host where the database is located (used for config)")
    parser.add_argument("--database", help="The database name (used for config and folder)")

    args = parser.parse_args()
    if not args.database:
        args.database = input("Enter the database name: ")
    print(f"Using database: {args.database}")
    main(args.host, args.database)


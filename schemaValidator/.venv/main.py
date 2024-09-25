import json
import os
import configparser
import time
from datetime import datetime

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL


# Add the DB2 driver path
os.add_dll_directory(
    'C:\\Users\\MITDeepanraj\\PycharmProjects\\schemaValidator\\.venv\\Lib\\site-packages\\clidriver\\bin')

# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')

source = config['COMPARISON']['SOURCE']
target = config['COMPARISON']['TARGET']

# Database connection details for Source DB
source_db = URL.create(
    drivername=config[source]['driver'],
    username=config[source]['username'],
    password=config[source]['password'],
    host=config[source]['host'],
    port=int(config[source]['port']),
    database=config[source]['database']
)

# Database connection details for Target DB
target_db = URL.create(
    drivername=config[target]['driver'],
    username=config[target]['username'],
    password=config[target]['password'],
    host=config[target]['host'],
    port=int(config[target]['port']),
    database=config[target]['database']
)

# Create engines
source_engine = create_engine(source_db)
target_engine = create_engine(target_db)

# Inspectors
source_inspector = inspect(source_engine)
target_inspector = inspect(target_engine)


def get_table_schema(inspector, table_name):
    columns = inspector.get_columns(table_name)
    primary_keys = inspector.get_pk_constraint(table_name)
    foreign_keys = inspector.get_foreign_keys(table_name)
    unique_constraints = inspector.get_unique_constraints(table_name)

    schema = {}

    # Add column information
    for column in columns:
        column_name = column['name']
        column_type = column['type']
        column_type_str = str(column_type).upper()  # Ensure type is a string and uppercase
        column_length = column_type.length if hasattr(column_type, 'length') else None
        column_data = {
            "datatype": column_type_str
        }
        if column_length:
            if isinstance(column_length, str):
                length_info = column_length.strip()
            elif isinstance(column_length, int):
                length_info = str(column_length)
            else:
                length_info = None

            if length_info:
                if "," in length_info:
                    precision, scale = length_info.split(",")
                    column_data["precision"] = int(precision.strip())
                    column_data["scale"] = int(scale.strip())
                else:
                    column_data["length"] = int(length_info.strip())

        # Add default value if available
        if column.get('default'):
            column_data['default'] = column['default']

        # Add nullability
        column_data['is_nullable'] = column['nullable']

        schema[column_name] = column_data

    # Add primary key constraint
    if primary_keys:
        schema['primary_key'] = primary_keys['constrained_columns']

    # Add foreign key constraints
    if foreign_keys:
        schema['foreign_keys'] = []
        for fk in foreign_keys:
            schema['foreign_keys'].append({
                'column': fk['constrained_columns'],
                'referenced_table': fk['referred_table'],
                'referenced_columns': fk['referred_columns']
            })

    # Add unique constraints
    if unique_constraints:
        schema['unique_constraints'] = [uc['column_names'] for uc in unique_constraints]

    # Add check constraints, but handle NotImplementedError
    try:
        check_constraints = inspector.get_check_constraints(table_name)
        if check_constraints:
            schema['check_constraints'] = [cc['sqltext'] for cc in check_constraints]
    except NotImplementedError:
        print(f"Check constraints not supported for table: {table_name}")

    return schema



def get_view_schema(inspector, view_name):
    # For views, we might not need detailed schema, but let's fetch columns as example
    columns = inspector.get_columns(view_name)
    schema = {}
    for column in columns:
        column_name = column['name']
        column_type = column['type']
        column_type_str = str(column_type).upper()
        schema[column_name] = {
            "datatype": column_type_str
        }
    return schema


def get_function_schema(engine, schema_name, function_name):
    print("HERE", engine, schema_name, function_name)
    query = text("""
        SELECT ROUTINE_NAME, ROUTINE_DEFINITION
        FROM SYSIBM.SYSCOMMENTS
        WHERE ROUTINE_SCHEMA = :schema_name
        AND ROUTINE_NAME = :function_name
        AND ROUTINE_TYPE = 'FUNCTION'
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {'schema_name': schema_name, 'function_name': function_name})
        row = result.fetchone()
        if row:
            return {
                function_name: {
                    "definition": row['ROUTINE_DEFINITION']
                }
            }
        return {}


def get_stored_procedure_schema(engine, schema_name, proc_name):
    print("HERE", engine, schema_name, proc_name)
    query = text("""
        SELECT ROUTINE_NAME, ROUTINE_DEFINITION
        FROM SYSIBM.SYSCOMMENTS
        WHERE ROUTINE_SCHEMA = :schema_name
        AND ROUTINE_NAME = :proc_name
        AND ROUTINE_TYPE = 'PROCEDURE'
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {'schema_name': schema_name, 'proc_name': proc_name})
        row = result.fetchone()
        if row:
            return {
                proc_name: {
                    "definition": row['ROUTINE_DEFINITION']
                }
            }
        return {}


def get_functions(engine, schema_name):
    query = text("""
        SELECT name FROM SYSIBM.SYSFUNCTIONS WHERE DEFINER = :schema_name;
    """)
    print('query: ', query)
    with engine.connect() as conn:
        result = conn.execute(query, {'schema_name': schema_name})
        functions = [row[0] for row in result]
        return functions


def get_stored_procedures(engine, schema_name):
    query = text("""
        SELECT name FROM SYSIBM.SYSPROCEDURES WHERE DEFINER = :schema_name;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {'schema_name': schema_name})
        procedures = [row[0] for row in result]
        return procedures


def get_schema(engine, schema_name, item_name, schema_type):
    if schema_type == 'tables':
        return get_table_schema(inspect(engine), item_name)
    elif schema_type == 'views':
        return get_view_schema(inspect(engine), item_name)
    elif schema_type == 'functions':
        return get_function_schema(engine, schema_name, item_name)
    elif schema_type == 'stored_procedures':
        return get_stored_procedure_schema(engine, schema_name, item_name)
    else:
        raise ValueError(f"Invalid schema type: {schema_type}")


def format_schema_for_json(schema):
    formatted_schema = {}
    for item_name, item_info in schema.items():
        if isinstance(item_info, dict):
            # Handle column definitions or other dictionary-based schema items
            if "definition" in item_info:
                item_data = {"definition": item_info["definition"]}
            else:
                item_data = {"datatype": item_info.get("datatype", "").split("(")[0].strip().lower()}
                length_info = item_info.get("datatype", "").split("(")[1][:-1] if len(
                    item_info.get("datatype", "").split("(")) > 1 else None

                if length_info:
                    if "," in length_info:
                        precision, scale = length_info.split(",")
                        item_data["precision"] = int(precision.strip())
                        item_data["scale"] = int(scale.strip())
                    else:
                        item_data["length"] = int(length_info.strip())

                if "default" in item_info:
                    item_data["default"] = item_info["default"]

                item_data["is_nullable"] = item_info.get("is_nullable", None)

        elif isinstance(item_info, list):
            # Handle lists, which are likely constraints or keys
            item_data = item_info

        else:
            # Handle unexpected types (optional, depending on your schema)
            item_data = str(item_info)

        formatted_schema[item_name] = item_data

    return formatted_schema



def save_schema_to_json(schema_data, output_file, schema_type):
    with open(output_file, 'w') as json_file:
        json.dump({schema_type: schema_data}, json_file, indent=4)
    print(f"{schema_type} schema saved to '{output_file}'.")


def read_lookup_file(lookup_file):
    with open(lookup_file, 'r') as file:
        items = [line.strip() for line in file if line.strip()]
    return items


def compare_schemas(source_schema, target_schema):
    differences = {}
    for item_name in source_schema:
        if item_name in target_schema:
            source_item_schema = source_schema[item_name]
            target_item_schema = target_schema[item_name]
            item_differences = []
            for column_name in source_item_schema:
                if column_name not in target_item_schema:
                    item_differences.append(f"Column '{column_name}' missing in target schema")
                elif source_item_schema[column_name] != target_item_schema[column_name]:
                    item_differences.append(
                        f"Column '{column_name}' mismatch: {source_item_schema[column_name]} != {target_item_schema[column_name]}")
            if item_differences:
                differences[item_name] = item_differences
        else:
            differences[item_name] = ["Missing in target schema"]
    for item_name in target_schema:
        if item_name not in source_schema:
            differences[item_name] = ["Missing in source schema"]
    return differences


def main():
    op_schema_name = config[source]['schema_name']
    cl_schema_name = config[target]['schema_name']
    output_dir = config['output']['directory']
    comparison_types = config['COMPARISON']['compare'].split(",")  # Split comparison types by comma

    timestamp = datetime.now().strftime("SchemaValidator_%Y%m%d_%H%M%S")
    output_dir_with_timestamp = os.path.join(output_dir, timestamp)
    os.makedirs(output_dir_with_timestamp, exist_ok=True)

    lookup_file = config['COMPARISON'].get('lookup_file', 'no').lower()

    # Define lookup files for each type
    lookup_files = {
        'tables': config['COMPARISON'].get('table_lookup_file', ''),
        'views': config['COMPARISON'].get('view_lookup_file', ''),
        'functions': config['COMPARISON'].get('function_lookup_file', ''),
        'stored_procedures': config['COMPARISON'].get('stored_procedure_lookup_file', '')
    }

    for comparison_type in comparison_types:
        comparison_type = comparison_type.strip()
        print(f"Starting comparison for {comparison_type}...")  # Debugging statement

        # Create a subfolder based on comparison type
        output_dir_for_comparison = os.path.join(output_dir_with_timestamp, comparison_type)
        os.makedirs(output_dir_for_comparison, exist_ok=True)

        source_schema = {}
        target_schema = {}

        use_lookup_file = lookup_file != 'no'
        lookup_file_path = lookup_files.get(comparison_type, '')

        if use_lookup_file and os.path.exists(lookup_file_path):
            items_source = read_lookup_file(lookup_file_path)
            items_target = items_source
        else:
            if comparison_type == 'tables':
                items_source = source_inspector.get_table_names(schema=op_schema_name)
                items_target = target_inspector.get_table_names(schema=cl_schema_name)
            elif comparison_type == 'views':
                items_source = source_inspector.get_view_names(schema=op_schema_name)
                items_target = target_inspector.get_view_names(schema=cl_schema_name)
            elif comparison_type == 'functions':
                items_source = get_functions(source_engine, op_schema_name)
                items_target = get_functions(target_engine, cl_schema_name)
            elif comparison_type == 'stored_procedures':
                items_source = get_stored_procedures(source_engine, op_schema_name)
                items_target = get_stored_procedures(target_engine, cl_schema_name)
            else:
                raise ValueError(f"Invalid comparison type specified: {comparison_type}")

        for item_name in items_source:
            print(f"Processing source {comparison_type[:-1]}: {item_name}")  # Debugging statement
            schema = get_schema(source_engine, op_schema_name, item_name, comparison_type)
            formatted_schema = format_schema_for_json(schema)
            source_schema[item_name] = formatted_schema

        for item_name in items_target:
            print(f"Processing target {comparison_type[:-1]}: {item_name}")  # Debugging statement
            schema = get_schema(target_engine, cl_schema_name, item_name, comparison_type)
            formatted_schema = format_schema_for_json(schema)
            target_schema[item_name] = formatted_schema

        source_output_file = os.path.join(output_dir_for_comparison, f'SourceSchema_{comparison_type}.json')
        target_output_file = os.path.join(output_dir_for_comparison, f'TargetSchema_{comparison_type}.json')

        save_schema_to_json(source_schema, source_output_file, f"SourceSchema_{comparison_type.capitalize()}")
        save_schema_to_json(target_schema, target_output_file, f"TargetSchema_{comparison_type.capitalize()}")

        differences = compare_schemas(source_schema, target_schema)
        differences_output_file = os.path.join(output_dir_for_comparison, f'SchemaDifferences_{comparison_type}.json')
        save_schema_to_json(differences, differences_output_file, "SchemaDifferences")

        print(f"Completed comparison for {comparison_type}.\n")  # Debugging statement


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Time taken: {time_taken:.2f} seconds")

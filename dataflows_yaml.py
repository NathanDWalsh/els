import pyodbc
import pandas as pd

def get_dataframe_from_sql(source):
    db_type = source["type"]
    server = source["server"]
    database = source["database"]
    table = source["table"]

    # Define the connection string based on the database type
    if db_type == "mssql":
        connection_string = f"Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;"
    elif db_type == "postgres":
        connection_string = f"Driver={{PostgreSQL}};Server={server};Database={database};"
    elif db_type == "duckdb":
        connection_string = f"Driver={{DuckDB}};Database={database};"

    try:
        # Connect to the database
        conn = pyodbc.connect(connection_string)

        # Create a cursor
        cursor = conn.cursor()

        # Execute the SQL query
        query = f"SELECT * FROM {table}"
        cursor.execute(query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Get the column names
        column_names = [column[0] for column in cursor.description]

        # Create a pandas DataFrame from the rows and column names
        df = pd.DataFrame.from_records(rows, columns=column_names)

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Return the DataFrame
        return df

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")
        return None


import yaml
import os

def load_store_info(eldef, store_id):
    
    if store_id in eldef:
        store_info = eldef[store_id]
        return store_info
    else:
        return None

def load_yaml_file(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def get_file_type_from_filename(filename):
    file_parts = filename.split('.')
    if len(file_parts) >= 2:
        file_type = file_parts[-2]
    else:
        file_type = ""
    return file_type

def get_file_type_from_path(file_path):
    _, file_extension = os.path.splitext(file_path)
    return file_extension

eldef = load_yaml_file('extract-load.yml')

source = load_store_info(eldef,'source')
target = load_store_info(eldef,'target')

if source is not None:
    source_type = source['type']

    if source_type in ('mssql', 'postgres', 'duckdb'):
        df = get_dataframe_from_sql(source)
    elif source_type in ('csv', 'excel'):
        file_path = source['file_path']

        # Code for file source
else:
    print("Invalid or incomplete source configuration in the YAML file.")

if df is not None:
    if target is None:
        print('no target defined, printing first 100 rows:')
        print(df.head(100))
    else:
        if target['type'] in ('csv'):
            df.to_csv(target['file_path'],index=False)
        else:
            pass
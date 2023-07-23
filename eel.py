import os
import pandas as pd
import sqlalchemy as sa
import pyodbc

import multiprocessing
from joblib import Parallel, delayed

import fscon as fc


def get_load_parallel_string(load_parallel):
    return 'parallel' if load_parallel else 'serial'


def get_db_connection_string(source):
    db_type = source["type"]
    server = source["server"]
    database = source["database"]

    # Define the connection string based on the database type
    if db_type == "mssql":
        # connection_string = f"Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;"
        connection_string = f"mssql+pyodbc://localhost/bitt?driver=ODBC+Driver+17+for+SQL+Server"
    elif db_type == "postgres":
        connection_string = f"Driver={{PostgreSQL}};Server={server};Database={database};"
    elif db_type == "duckdb":
        connection_string = f"Driver={{DuckDB}};Database={database};"

    return connection_string


def get_dataframe_from_sql(source):
    table = source["table"]

    connection_string = get_db_connection_string(source)

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


def load_dataframe_structure_to_sql(df: pd.DataFrame, target: dict, add_cols: dict, sqeng):

    # Create a new DataFrame with only one row for table creation
    df_create = df.head(1).copy()
    table_name = target["table"]

    if 'schema' in target:
        schema = target['schema']
        schema_table_name = schema + '.[' + table_name + ']'
    else:
        schema_table_name = table_name
        schema = None

    sqeng.execute(sa.text(f"drop table if exists {schema_table_name}"))
    # sqeng.connection.commit()

    # Use the first row to create the table structure
    df_create.to_sql(table_name, sqeng, schema=schema, index=False)
    # sqeng.connection.commit()

    # Delete the temporary row from the table
    sqeng.execute(sa.text(f"DELETE FROM {schema_table_name}"))
    # sqeng.connection.commit()

    row_index_col = None
    if add_cols:
        if 'row_index' in add_cols:
            row_index_col = add_cols['row_index']

    # Add an identity column to the table
    sqeng.execute(sa.text(
        f"ALTER TABLE {schema_table_name} ADD {row_index_col} int identity(1,1) PRIMARY KEY "))

    sqeng.connection.commit()


def load_dataframe_to_sql(df: pd.DataFrame, target: dict, add_cols: dict):
    # conns = 'mssql+pyodbc://localhost/bi' + config['Country']['Country'] + '?driver=ODBC+Driver+17+for+SQL+Server'
    connection_string = get_db_connection_string(target)
    sqeng = sa.create_engine(
        connection_string, fast_executemany=True).connect()

    schema = target.get('schema', None)
    if_exists = target.get('if_exists', None)
    table_consistency = check_table_consistency(
        sqeng, target["table"], schema, df, add_cols)
    if table_consistency == 0 or if_exists == 'replace':
        load_dataframe_structure_to_sql(df, target, add_cols, sqeng)
        table_consistency = 1
        if_exists = 'append'
    if table_consistency >= 0 and if_exists == 'truncate':
        pass  # TODO
    if table_consistency >= 0 or (target['consistency'] == 'ignore'):
        kwargs = target.get('to_sql', {})
        df.to_sql(target["table"], sqeng, schema,
                  index=False, if_exists=if_exists, **kwargs)
        sqeng.connection.commit()
        print(target["table"] + " Data saved successfully.")
    elif table_consistency == -1:
        print(target["table"] + " Inconsistent, not saved.")


def check_table_consistency(engine, table_name, schema_name, dataframe, add_cols):
    inspector = sa.inspect(engine)

    # Check if the table exists
    if not inspector.has_table(table_name, schema_name):
        return 0  # Table does not exist

    # Get the table columns
    table_columns = inspector.get_columns(table_name, schema_name)

    # Compare the column names and types
    dataframe_columns = dataframe.columns.tolist()
    dataframe_columns.extend(add_cols.values())

    if len(table_columns) != len(dataframe_columns):
        return -1  # Table has different field names
    for table_col, df_col in zip(table_columns, dataframe_columns):
        if table_col["name"] != df_col:
            return -1  # Table has different field names
        # TODO fix this: create equivalencies between df types and possible sql types
        # if str(table_col["type"]) != str(dataframe[df_col].dtype):
        #     return -1  # Table has different field types

    return 1  # Table exists and has the same field names and types


def generate_create_table_query(table, dataframe):
    columns = ", ".join(
        [f"{col} {get_sql_data_type(dataframe[col].dtype)}" for col in dataframe.columns])
    query = f"CREATE TABLE {table} ({columns})"
    return query


def get_sql_data_type(dtype):
    if dtype == "int64":
        return "INT"
    elif dtype == "float64":
        return "FLOAT"
    elif dtype == "bool":
        return "BIT"
    elif dtype == "object":
        return "VARCHAR(MAX)"
    elif dtype == "datetime64":
        return "DATETIME"
    else:
        return "VARCHAR(MAX)"


def generate_insert_query(table, dataframe):
    columns = ", ".join(dataframe.columns)
    placeholders = ", ".join(["?" for _ in dataframe.columns])
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    return query


def get_dataframe_from_csv(file_path, **kwargs):
    df = pd.read_csv(file_path, **kwargs)
    return df

# def load_yaml_file(file_path):
#     with open(file_path, 'r') as file:
#         data = yaml.safe_load(file)
#     return data


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


def get_file_base_ext(file_path):
    file_name, file_extension = os.path.splitext(os.path.basename(file_path))
    return [file_name, file_extension[1:]]


def table_exists(sqeng: sa.engine, table_name: str):
    inspector = sa.inspect(sqeng)
    return inspector.has_table(table_name)


def merge_source_kwargs(source, source_key):
    kwargs = {}

    if 'nrows' in source:
        kwargs['nrows'] = source['nrows']

    # Merge source_kwargs with kwargs (source_kwargs takes precedence)
    if source_key in source:
        kwargs.update(source[source_key])

    if 'nrows' in kwargs:
        if not kwargs['nrows']:
            kwargs['nrows'] = None

    return kwargs


def ingest(elfile, eldef):

    target = eldef.get('target', None)
    source = eldef.get('source', {})
    add_cols = eldef.get('add_cols', None)

    # elfile_base, elfile_ext = get_file_base_ext(elfile)
    # if source == None:
    #     source = {}
    # if 'file_path' not in source:
    #     source['file_path'] = elfile
    # if 'type' not in source:
    #     source['type'] = elfile_ext

    # if source != None:
    source_type = source['type']

    if source_type in ('mssql', 'postgres', 'duckdb'):
        df = get_dataframe_from_sql(source)
    elif source_type in ('csv', 'tsv'):
        kwargs = merge_source_kwargs(source, 'read_csv')
        if source_type == 'tsv':
            kwargs['sep'] = '\t'
        df = get_dataframe_from_csv(source['file_path'], **kwargs)
    elif source_type in ('excel', 'xls', 'xlsx', 'xlsb', 'xlsm'):
        pass
    if df is not None:
        if target is None:
            print('no target defined, printing first 100 rows:')
            print(df.head(100))
        else:
            if target['type'] in ('csv'):
                df.to_csv(target['file_path'], index=False)
            elif target['type'] in ('mssql', 'postgres', 'duckdb'):
                # if 'table' not in target:
                #     target['table'] = elfile_base
                load_dataframe_to_sql(df, target, add_cols)
            else:
                pass


class eel(fc.fscon):

    def __init__(self, path, config_extension='.eel.yml'):
        super().__init__(path, config_extension)
        self.container_hierarchy = self.get_custom_hierarchy(self.file_hierarchy, folder_attributes={'type': 'container', 'is_homog': {'config_path': ['source', 'is_homog'], 'default': False}, 'load_parallel': {
                                                             'config_path': ['source', 'load_parallel'], 'default': False}}, file_attributes={'type': {'config_path': ['source', 'type']}})
        self.taskflow = self.get_taskflow()
        self.num_cores = multiprocessing.cpu_count()

    def get_taskflow(self):
        return self.get_taskflow_container(self.container_hierarchy)[0]

    def get_taskflow_container(self, container_hierarchy):
        result = []
        for item_path, item_cs in container_hierarchy.items():
            if item_cs['type'] == 'container':
                contents = self.get_taskflow_container(item_cs['contents'])
                item_res = (get_load_parallel_string(
                    item_cs['load_parallel']), contents)
                if item_cs['is_homog']:
                    table = fc.get_dict_item(
                        self.configs, [item_path, 'target', 'table'])
                    for c in contents:
                        self.configs[c]['target']['table'] = table
                    if fc.get_dict_item(self.configs, [item_path, 'target', 'if_exists']) == 'replace':
                        for c in contents[1:]:
                            self.configs[c]['target']['if_exists'] = 'append'
                        item_res = 'serial', [contents[0], (get_load_parallel_string(
                            item_cs['load_parallel']), contents[1:])]
            else:
                item_res = item_path

            result.append(item_res)

        return result

    def process_tasks(self):
        self.process_task(self.taskflow)

    def process_task(self, task):
        if isinstance(task, tuple):
            parallelizable = True if task[0] == 'parallel' else False
            subtasks = task[1]
            if parallelizable:
                Parallel(n_jobs=self.num_cores)(
                    delayed(self.process_task)(t) for t in subtasks)
            else:
                for t in subtasks:
                    self.process_task(t)
        else:
            task_config = self.configs[task]
            ingest(task, task_config)

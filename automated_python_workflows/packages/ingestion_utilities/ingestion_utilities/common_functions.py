import yaml
import pandas as pd

from snowflake.connector.connection import SnowflakeConnection

def get_unique_tables_in_clarity_sources_manual(clarity_sources_manual_path):
    with open(clarity_sources_manual_path, 'r') as file:
        clarity_sources_manual = yaml.safe_load(file)
    unique_tables = []
    for table in clarity_sources_manual['sources'][0]['tables']:
        if table['name'][-7:] == "_DELETE": #_DELETE tables are duplicates
            continue
        unique_tables.append(table['name'])

    return unique_tables

def concat_list_elements_for_snowflake_query(views: list) -> str:
    return ','.join([f"'{view}'" for view in views])

def execute_snowflake_query(connection: SnowflakeConnection, query: str) -> pd.DataFrame:
    try:
        cursor = connection.cursor().execute(query)
        df = pd.DataFrame.from_records(iter(cursor), columns=[x[0] for x in cursor.description])
        cursor.close()
        return df
    except Exception as e:
        print(f"error {e} with executing query {query}")
import os
import logging
import re
import shutil
import pandas as pd

from ingestion_utilities.connections import connect_to_snowflake
from ingestion_utilities.processing_files import process_single_csv_input_from_folder
from ingestion_utilities.common_functions import concat_list_elements_for_snowflake_query, execute_snowflake_query

from clarity_table_ingestion.src.config import PROJECT_FOLDER, PROJECT_OUTPUT_FOLDER

logger = logging.getLogger(os.path.basename(__file__))

INPUT_CSV_FOLDER = os.path.join(PROJECT_FOLDER, 'input')
OUTPUT_VALIDATION_CSV_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, 'input_tables_validation')

def _table_prefix_check(tables) -> None:
    for table in tables:
        if table.startswith("V_"):
            logger.error(f"table {table} is prefixed with V_ for a view but this program can't handle that yet!")
            raise Exception(f"table {table} is prefixed with V_ for a view but this program can't handle that yet!")
        elif table.startswith("X_"):
            logger.warning(f"table {table} is prefixed with X_ but this program isn't sure if it can can't handle that yet!")
        else:
            pass

def _get_primary_keys(snowflake_connection, tables):
    table_names_str = concat_list_elements_for_snowflake_query(tables)
    query = f'''
    select table_name, column_name, primary_key
    from MDP_RAW_DEV.GENERAL_MGMT.COLUMN_CONFIG
    where table_name in ({table_names_str})
    and primary_key = 'true'
    order by table_name asc; 
    '''
    df = execute_snowflake_query(snowflake_connection, query)
    #consolidate and reformat multiple primary keys to one record
    tables_and_pks = {'TABLE_NAME': [], 'PRIMARY_KEYS': []}
    for table in tables:
        tables_and_pks['TABLE_NAME'].append(table)
        tables_and_pks['PRIMARY_KEYS'].append(list(df.loc[df['TABLE_NAME'] == table]["COLUMN_NAME"]))

    ingestion_analysis = pd.DataFrame.from_dict(tables_and_pks)
    ingestion_analysis = ingestion_analysis.set_index("TABLE_NAME")
    logger.debug(f"ingestion_analysis in _get_primary_keys: {ingestion_analysis}")

    assert (len(ingestion_analysis) == len(tables))

    return ingestion_analysis

def _get_source_table_status(snowflake_connection, tables):
    table_names_str = concat_list_elements_for_snowflake_query(tables)
    query = f'''
    select TABLE_NAME, IS_ENABLED
    from MDP_RAW_DEV.GENERAL_MGMT.SOURCE_TABLE
    where SOURCE_KEY = 1
    and table_name in ({table_names_str})
    order by table_name asc; 
    '''
    df = execute_snowflake_query(snowflake_connection, query)
    tables_enabled_in_uat = {"TABLE_NAME": [], "SOURCE_TABLE_VALIDATION": []}
    for table in tables:
        tables_enabled_in_uat["TABLE_NAME"].append(table)
        #ENABLED_IN_UAT scenario
        if len(df[(df["TABLE_NAME"] == table) & (df["IS_ENABLED"] == True)]) > 0:
            tables_enabled_in_uat["SOURCE_TABLE_VALIDATION"].append("ENABLED_IN_UAT")
        #IN SOURCE_TABLE BUT NOT ENABLED_IN_UAT scenario
        elif len(df[(df["TABLE_NAME"] == table) & (df["IS_ENABLED"] == False)]) > 0:
            tables_enabled_in_uat["SOURCE_TABLE_VALIDATION"].append("IN SOURCE_TABLE BUT NOT ENABLED_IN_UAT")
        #NOT IN SOURCE_TABLE scenario
        elif table not in set(df["TABLE_NAME"]):
            tables_enabled_in_uat["SOURCE_TABLE_VALIDATION"].append("NOT IN SOURCE_TABLE")
        else:
            raise Exception(f"a scenario was not accounted for in the source table validation! with {table}")
        
    ingestion_analysis = pd.DataFrame.from_dict(tables_enabled_in_uat)
    ingestion_analysis = ingestion_analysis.set_index("TABLE_NAME")
    logger.debug(f"ingestion_analysis in _get_source_table_status: {ingestion_analysis}")
    assert (len(ingestion_analysis) == len(tables))

    return ingestion_analysis

def _get_landing_table_status_one(snowflake_connection, tables):
    table_names_str = concat_list_elements_for_snowflake_query([f"RAW_{table}" for table in tables])
    query = f'''
    SELECT TABLE_NAME
    FROM MDP_RAW_DEV.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME IN ({table_names_str});
    '''
    df = execute_snowflake_query(snowflake_connection, query)
    landing_table_status = {"TABLE_NAME": [], "LANDING_TABLE_STATUS": []}
    for table in tables:
        raw_tbl_name = f'RAW_{table}'
        landing_table_status["TABLE_NAME"].append(table)
        #landing tables exist scenario
        if len(df[(df["TABLE_NAME"] == raw_tbl_name)]) > 0:
            landing_table_status["LANDING_TABLE_STATUS"].append("LANDING_TABLE_CREATED")
        #landing tables do not exist
        elif raw_tbl_name not in set(df["TABLE_NAME"]):
            landing_table_status["LANDING_TABLE_STATUS"].append("NO_LANDING_TABLE")
        else:
            raise Exception(f"a scenario was not accounted for in the source table validation! with {table}")

    landing_tbl_analysis = pd.DataFrame.from_dict(landing_table_status)
    landing_tbl_analysis = landing_tbl_analysis.set_index("TABLE_NAME")
    logger.debug(f"landing_tbl_analysis in _get_landing_table_status_one: {landing_tbl_analysis}")
    assert (len(landing_tbl_analysis) == len(tables))

    return landing_tbl_analysis

def _get_ingestion_type(snowflake_connection, tables):
    table_names_str = concat_list_elements_for_snowflake_query(tables)
    query = f'''
        SELECT 
        TABLE_NAME,
        INGESTION_TYPE
        FROM MDP_RAW_DEV.GENERAL_MGMT.SOURCE_TABLE_CONFIG
        WHERE IS_ENABLED = TRUE 
        AND TARGET_DIRECTORY LIKE '%claritynonprod.ahc.root.loc%'
        AND TABLE_NAME IN ({table_names_str}); 
    '''
    df = execute_snowflake_query(snowflake_connection, query)
    load_type_col_name = "INGESTION_TYPE"
    load_type_dict = {"TABLE_NAME": [], "TABLE_LOAD_TYPE": []}
    for table in tables:
        load_type_dict["TABLE_NAME"].append(table)
        tables_with_query_results = set(df["TABLE_NAME"])
        if table not in tables_with_query_results:
            load_type_dict["TABLE_LOAD_TYPE"].append("NO RESULTS FROM QUERY")
        else:
            #there can be duplicated rows with the same load type in the data
            list_of_load_types = list(df[df["TABLE_NAME"] == table][load_type_col_name].values)
            list_of_load_types = list(set(list_of_load_types))
            if len(list_of_load_types) > 1:
                raise Exception(f"{table} has multiple different load types associated with it!")
            elif len(list_of_load_types) == 0:
                raise Exception(f"{table} has no load types associated with it in load_type_dict!")
            else:
                load_type_dict["TABLE_LOAD_TYPE"].extend(list_of_load_types)

    ingestion_analysis = pd.DataFrame.from_dict(load_type_dict)
    ingestion_analysis = ingestion_analysis.set_index("TABLE_NAME")
    logger.debug(f"ingestion_analysis in _get_ingestion_type: {ingestion_analysis}")
    assert (len(ingestion_analysis) == len(tables))

    return ingestion_analysis

def _get_source_table_size(snowflake_connection, tables):
    table_names_str = concat_list_elements_for_snowflake_query(tables)
    query = f'''
        SELECT t.TABLE_NAME, ts.TOTAL_ROW_COUNT
        FROM (
        SELECT TABLE_NAME, MAX(AS_OF_TIME) AS MAX_AS_OF_TIME
        FROM MDP_RAW_DEV.GENERAL_MGMT.SOURCE_TABLE_SIZE
        WHERE SOURCE_KEY = 1
        AND TABLE_NAME IN ({table_names_str})
        GROUP BY TABLE_NAME
        ) t
        JOIN MDP_RAW_DEV.GENERAL_MGMT.SOURCE_TABLE_SIZE ts
        ON t.TABLE_NAME = ts.TABLE_NAME AND t.MAX_AS_OF_TIME = ts.AS_OF_TIME; 
    '''
    df = execute_snowflake_query(snowflake_connection, query)
    ROW_COUNT_COL_NAME = "TOTAL_ROW_COUNT"
    info_dict = {"TABLE_NAME": [], "SOURCE_ROW_COUNT": []}
    for table in tables:
        info_dict["TABLE_NAME"].append(table)
        tables_with_query_results = set(df["TABLE_NAME"])
        if table not in tables_with_query_results:
            info_dict["SOURCE_ROW_COUNT"].append("NO RESULTS FROM QUERY")
        else:
            list_results = list(df[df["TABLE_NAME"] == table][ROW_COUNT_COL_NAME].values)
            if len(list_results) > 1:
                logger.error(f"{table} has multiple records associated with it!")
                raise Exception(f"{table} has multiple records associated with it!")
            elif len(list_results) == 0:
                logger.error(f"{table} has no data associated with it!")
                raise Exception(f"{table} has no data associated with it!")
            else:
                info_dict["SOURCE_ROW_COUNT"].extend(list_results)

    ingestion_analysis = pd.DataFrame.from_dict(info_dict)
    ingestion_analysis = ingestion_analysis.set_index("TABLE_NAME")
    logger.debug(f"ingestion_analysis in _get_table_size: {ingestion_analysis}")
    assert (len(ingestion_analysis) == len(tables))

    return ingestion_analysis

def _get_uat_table_size(snowflake_connection, tables):
    """
    This is used to validate all the tables are in prod and have the proper record size
    """
    table_names_with_zc_replaced = [f"{re.sub(r'^ZC_', 'LKP_CLR_', table.upper())}" for table in tables]
    table_names_str = concat_list_elements_for_snowflake_query(table_names_with_zc_replaced)
    logger.debug(f"table_names_str in get_uat_table_size: {table_names_str}")
    queries = ['USE DATABASE MDP_REFINED_UAT;',
               'USE SCHEMA CLARITY_AAH;',
               f'''
                    SELECT
                    t.table_name,
                    COUNT(c.column_name) AS row_count
                    FROM
                    information_schema.tables t
                    JOIN information_schema.columns c
                        ON t.table_catalog = c.table_catalog
                        AND t.table_schema = c.table_schema
                        AND t.table_name = c.table_name
                    WHERE
                    t.table_catalog = 'MDP_REFINED_UAT'
                    AND t.table_schema = 'CLARITY_AAH'
                    AND t.table_name IN ({table_names_str})
                    GROUP BY
                    t.table_name;
                ''']

    for i, query in enumerate(queries):
        logger.debug(f"Executing query {i + 1}: {query}")
        sf_cursor = snowflake_connection.cursor()
        sf_cursor.execute(query)
        # If this is the last query, capture the results in a DataFrame
        if i == len(queries) - 1:
            df = execute_snowflake_query(snowflake_connection, query)
            logger.debug(f"df in get_uat_table_size: {df}")

    ROW_COUNT_COL_NAME = "ROW_COUNT"
    info_dict = {"TABLE_NAME": [], "UAT_ROW_COUNT": []}
    for table in tables:
        zc_converted_table_name = re.sub(r'^ZC_', 'LKP_CLR_', table)
        info_dict["TABLE_NAME"].append(table)
        tables_with_query_results = set()
        for table_name in df["TABLE_NAME"]:
            # Apply the zc to lkp_clr conversion to table list and results from df
            modified_table_name = re.sub(r'^LKP_CLR_', 'ZC_', table_name)
            tables_with_query_results.add(modified_table_name)

        if table not in tables_with_query_results:
            info_dict["UAT_ROW_COUNT"].append("NO RESULTS FROM QUERY")
        else:
            results = list(df[df["TABLE_NAME"] == zc_converted_table_name][ROW_COUNT_COL_NAME].values)
            if len(list_results) > 1:
                raise Exception(f"{table} has multiple records associated with it!")
            elif len(list_results) == 0:
                raise Exception(f"{table} has no data associated with it!")
            else:
                info_dict["UAT_ROW_COUNT"].extend(results)

    ingestion_analysis = pd.DataFrame.from_dict(info_dict)
    ingestion_analysis = ingestion_analysis.set_index("TABLE_NAME")
    logger.debug(f"ingestion_analysis in get_uat_table_size: {ingestion_analysis}")
    assert (len(ingestion_analysis) == len(tables))

    return ingestion_analysis

def _get_landing_table_status_delete_tables(snowflake_connection, validation_df) -> pd.DataFrame:
    #you can't leverage INCREMENTAL_TABLES from common_functions.py because the df is not created yet
    incremental_tables = list(validation_df[validation_df["TABLE_LOAD_TYPE"] == "INCREMENTAL"].index.values)
    if len(incremental_tables) == 0:
        return validation_df
    proper_increment_table_names = [f"RAW_{table}_DELETE" for table in incremental_tables]
    table_names_str = concat_list_elements_for_snowflake_query(proper_increment_table_names)
    query = f'''
    SELECT TABLE_NAME
    FROM MDP_RAW_DEV.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME IN ({table_names_str});
    '''
    df = execute_snowflake_query(snowflake_connection, query)
    df = df.set_index("TABLE_NAME")
    for table in incremental_tables:
        raw_tbl_name_delete = f'RAW_{table}_DELETE'
        #check for DELETE landing tables that do not exist
        if raw_tbl_name_delete not in set(df.index.values):
            validation_df.at[table, 'LANDING_TABLE_STATUS'] = 'NO_DELETE_LANDING_TABLE'
    logger.debug(f"validation_df in _get_landing_table_status_delete_tables: {validation_df}")

    return validation_df

def _create_validation_csv(validation_df, file_name, destination_folder_path):

    last_run_folder_path = destination_folder_path + r'\last_run'
    previous_runs_folder_path = destination_folder_path + r'\previous_runs'
    
    destination_csv_file_path = last_run_folder_path + f'\{file_name}-validation.csv'
    moved_file_path = previous_runs_folder_path + f'\{file_name}-validation.csv'

    #if there is already a file in the target folder, move it to the 'previous_runs_folder_path'
    if len(os.listdir(last_run_folder_path)) > 0:
        file_list = os.listdir(last_run_folder_path)
        full_paths = [os.path.join(last_run_folder_path, filename) for filename in file_list]
        for file in full_paths:
            if os.path.exists(moved_file_path):
                os.remove(moved_file_path)
            shutil.move(file, moved_file_path)

    with open(destination_csv_file_path, "w+") as file:
        #write header data
        file.write("Ready_to_create_model_files,")
        file.write("Table_name,")
        file.write("Load_type,")
        file.write("Primary_keys,")
        file.write("Source_table_validation,")
        file.write("Landing_table_validation,")
        file.write("Source_row_count,")
        file.write("UAT_row_count")
        file.write("\n")

        for table, row in validation_df.iterrows():
            ready_to_create_model_files = "NO"

            #logic to validate a table
            if row['SOURCE_TABLE_VALIDATION'] == 'ENABLED_IN_UAT' \
                and row['PRIMARY_KEYS'] != '' \
                and row['TABLE_LOAD_TYPE'] != "NO RESULTS FROM QUERY" \
                and row['LANDING_TABLE_STATUS'] == 'LANDING_TABLE_CREATED':
                
                    ready_to_create_model_files = "YES"
                
            #write data for each row in validation_df
            file.write(f"{ready_to_create_model_files},")
            file.write(f"{table},")
            file.write(f"{row['TABLE_LOAD_TYPE']},")
            file.write(f"{' '.join(row['PRIMARY_KEYS'])},")
            file.write(f"{row['SOURCE_TABLE_VALIDATION']},")
            file.write(f"{row['LANDING_TABLE_STATUS']},")
            file.write(f"{row['SOURCE_ROW_COUNT']},"),
            file.write(f"{row['UAT_ROW_COUNT']}")
            file.write(f"\n")

def validate_new_tables() -> None:
    logger.info("table_ingestion_validation started")
    file_name, tables = process_single_csv_input_from_folder(INPUT_CSV_FOLDER)
    #check for input tables this program can't handle yet
    _table_prefix_check(tables)

    snowflake_context_session = connect_to_snowflake()
    #getting data for validation
    pk_df = _get_primary_keys(snowflake_context_session, tables)
    ingestion_types_df = _get_ingestion_type(snowflake_context_session, tables)
    src_tbl_validation_df = _get_source_table_status(snowflake_context_session, tables)
    landing_tbl_validation_df = _get_landing_table_status_one(snowflake_context_session, tables)
    table_size_df = _get_source_table_size(snowflake_context_session, tables)
    uat_check = _get_uat_table_size(snowflake_context_session, tables)

    #merging data
    validation_df = pk_df.merge(src_tbl_validation_df, on="TABLE_NAME")
    validation_df = validation_df.merge(ingestion_types_df, on="TABLE_NAME")
    validation_df = validation_df.merge(landing_tbl_validation_df, on="TABLE_NAME")
    validation_df = validation_df.merge(table_size_df, on="TABLE_NAME")
    validation_df = validation_df.merge(uat_check, on="TABLE_NAME")

    #ensure the DELETE table was created when landing tables were built!
    validation_df = _get_landing_table_status_delete_tables(snowflake_context_session, validation_df)

    #writing validation data to cs for next script to use
    _create_validation_csv(validation_df, file_name, OUTPUT_VALIDATION_CSV_FOLDER)

    logger.info("table_ingestion_validation completed")

if __name__ == '__main__':
    validate_new_tables()
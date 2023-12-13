import os
import logging
import pandas as pd

from clarity_table_ingestion.src.config import VALIDATION_CSV_OUTPUT_FOLDER


logger = logging.getLogger(os.path.basename(__file__))

def clear_folder_contents(full_folder_path):
   try:
     files = os.listdir(full_folder_path)
     for file in files:
       file_path = os.path.join(full_folder_path, file)
       if os.path.isfile(file_path):
         os.remove(file_path)
   except OSError:
     print("Error occurred while deleting files or no files were present.")

# =================================================================================================
# VALIDATION FILE DETAILS
# =================================================================================================
def _get_validation_file_details():
    file_name = os.listdir(VALIDATION_CSV_OUTPUT_FOLDER)[0]
    file_path = os.path.join(VALIDATION_CSV_OUTPUT_FOLDER, file_name)

    validation_details = {
        "file_path": file_path,
        "file_name": file_name,
        "batch_id": file_name[:-15]
        }

    return validation_details

def get_all_tables_from_validation_file():
    validation_file_details = _get_validation_file_details()

    validation_df = pd.read_csv(validation_file_details["file_path"])

    return list(validation_df["Table_name"].values)

def get_validation_df():
    validation_file_details = _get_validation_file_details()

    validation_df = pd.read_csv(validation_file_details["file_path"])

    validation_df = validation_df.set_index("Table_name")

    return validation_df

def get_batch_id():
    validation_file_details = _get_validation_file_details()

    return validation_file_details["batch_id"]

def get_validation_csv_path():
    validation_file_details = _get_validation_file_details()

    return validation_file_details["file_path"]

def get_validated_tables_from_validation_file():
    validation_df = get_validation_df()
    return list(validation_df[validation_df["Ready_to_create_model_files"] == "YES"].index.values)

def get_incremental_tables_from_validation_file():
    validation_df = get_validation_df()
    return list(validation_df[validation_df["Load_type"] == "INCREMENTAL"].index.values)

# =================================================================================================
# LANDING TABLES
# =================================================================================================

def _get_delete_landing_tables() -> list:

    delete_landing_tables = []
    for incremental_table in INCREMENTAL_TABLES:
        delete_landing_tables.append(f"{incremental_table}_DELETE")
    logger.debug(f"found {len(delete_landing_tables)} delete_landing_tables: {delete_landing_tables}")
    
    return delete_landing_tables

def get_all_landing_tables() -> list:

    delete_landing_tables = _get_delete_landing_tables()
    all_landing_tables = INPUT_TABLES + delete_landing_tables
    logger.debug(f"found {len(all_landing_tables)} all_landing_tables: {all_landing_tables}")
    
    return all_landing_tables

# =================================================================================================
#output variables used in other scripts
# =================================================================================================

VALIDATION_FILE_PATH = get_validation_csv_path()
BATCH_ID = get_batch_id()

# VALIDATION_CSV_PATH = TODO
VALIDATION_DF = get_validation_df()
INPUT_TABLES = get_all_tables_from_validation_file()
VALIDATED_TABLES = get_validated_tables_from_validation_file()
INCREMENTAL_TABLES = get_incremental_tables_from_validation_file()

LANDING_TABLES = get_all_landing_tables()
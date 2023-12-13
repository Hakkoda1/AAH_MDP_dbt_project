import os

CURRENT_DIRECTORY = os.getcwd()

PROJECT_FOLDER = os.path.join(CURRENT_DIRECTORY, r'automated_python_workflows\packages\clarity_table_ingestion\clarity_table_ingestion')

LOG_PATH = os.path.join(PROJECT_FOLDER, 'logs', 'clarity_table_ingestion.log')

PROJECT_OUTPUT_FOLDER = os.path.join(PROJECT_FOLDER, 'outputs')

PROJECT_OUTPUT_FOLDER_FIXING_INVALIDATED_TABLES = os.path.join(PROJECT_OUTPUT_FOLDER, 'fixing_invalidated_input_tables')
VALIDATION_CSV_OUTPUT_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, 'input_tables_validation', 'last_run')
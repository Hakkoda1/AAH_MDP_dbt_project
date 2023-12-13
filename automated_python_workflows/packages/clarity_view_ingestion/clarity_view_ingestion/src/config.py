import os

CURRENT_DIRECTORY = os.getcwd()

PROJECT_FOLDER = os.path.join(CURRENT_DIRECTORY, r'automated_python_workflows\packages\clarity_view_ingestion\clarity_view_ingestion')
PROJECT_INPUT_FOLDER = os.path.join(PROJECT_FOLDER, 'inputs')
PROJECT_OUTPUT_FOLDER = os.path.join(PROJECT_FOLDER, 'outputs')

LOG_PATH = os.path.join(PROJECT_FOLDER, 'logs', 'clarity_table_ingestion.log')

SRC_VIEW_SCRIPTS_OUTPUT_PATH = os.path.join(PROJECT_OUTPUT_FOLDER, 'source_veiw_scripts.yaml')
CLEANED_VIEWS_QUERY_MAPPING_PATH = os.path.join(PROJECT_OUTPUT_FOLDER, 'phase2_cleaned_views_query_mapping.json')
MASTER_TABLE_REF_DICT_OUTPUT_PATH = os.path.join(PROJECT_OUTPUT_FOLDER, 'phase3_master_table_ref_mapping.json')
DEPENDENCY_MAPPING_PATH = os.path.join(PROJECT_OUTPUT_FOLDER, 'dependency_mapping.json')
PHASE3_VIEWS_QUERY_MAPPING_PATH = os.path.join(PROJECT_OUTPUT_FOLDER, 'phase3_replaced_tble_refs_views_query_mapping.json')
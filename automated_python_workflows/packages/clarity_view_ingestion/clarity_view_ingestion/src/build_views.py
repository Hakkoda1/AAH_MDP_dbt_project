import os

from ingestion_utilities.processing_files import process_single_csv_input_from_folder
from ingestion_utilities.dbt_functions import generate_dbt_build_command_str

from clarity_view_ingestion.src.config import PROJECT_INPUT_FOLDER

READY_VIEWS_FOLDER_PATH = os.path.join(PROJECT_INPUT_FOLDER, 'views_ready_to_build')

if __name__ == "__main__":
    _, views_ready_to_build = process_single_csv_input_from_folder(READY_VIEWS_FOLDER_PATH)
    dbt_command = generate_dbt_build_command_str(views_ready_to_build)
    print(dbt_command)
import logging
import os

from ingestion_utilities.dbt_functions import (
    execute_dbt_command,
    generate_dbt_build_command_str,
    generate_dbt_build_command_list
)

from clarity_table_ingestion.src.config import PROJECT_OUTPUT_FOLDER
from clarity_table_ingestion.src.utilities.common_functions import VALIDATED_TABLES, BATCH_ID

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)

DBT_BUILD_LOG_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, 'dbt_build')

def _write_dbt_output(write_folder_path: str, captured_txt: str) -> None:

    dbt_build_log_file_path = write_folder_path + f'\{BATCH_ID}-dbt-build-log.txt'
    logger.debug(f"about to write dbt output to {dbt_build_log_file_path}")
    with open(dbt_build_log_file_path, 'w') as file:
        file.write(captured_txt)
        logger.debug(f"successfully wrote output to {dbt_build_log_file_path}")

def build_models() -> None:
    logger.info(f"dbt build script starting")
    command_str = generate_dbt_build_command_str(VALIDATED_TABLES)
    logger.info(f"preparing to execute {command_str}\n")
    dbt_build_command_args = generate_dbt_build_command_list(command_str)
    captured_txt, response = execute_dbt_command(dbt_build_command_args)
    logger.debug(f"dbt build script response: {response}")
    _write_dbt_output(DBT_BUILD_LOG_FOLDER, captured_txt)
    logger.info(f"dbt build script completed check results at {DBT_BUILD_LOG_FOLDER}")

if __name__ == '__main__':
    generate_dbt_build_command_str(VALIDATED_TABLES)
    build_models()
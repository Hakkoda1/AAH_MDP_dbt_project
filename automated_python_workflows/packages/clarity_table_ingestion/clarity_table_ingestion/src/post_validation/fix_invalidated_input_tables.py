import logging
import os

from clarity_table_ingestion.src.post_validation.fixing_invalidated_input_tables.add_to_clarity_sources import add_missing_tables_to_clarity_sources
from clarity_table_ingestion.src.post_validation.fixing_invalidated_input_tables.stage_external_sources import stage_external_sources

from clarity_table_ingestion.src.utilities.common_functions import LANDING_TABLES

logger = logging.getLogger(os.path.basename(__file__))

def build_landing_tables() -> None:
    """
    Builds landing tables and snowpipes for all tables in VALIDATION_DF.
    For incremental tables, also builds the delete landing table and snowpipe.
    TODO: add logic to only build landing tables for tables that are not already built.
    """
    
    logger.info("build landing tables started")
    stage_external_sources_results: dict = stage_external_sources()
    
    if stage_external_sources_results['success'] == False:
        logger.error(f"failed to stage external sources with {LANDING_TABLES} : {stage_external_sources_results['exception']}")

    else:
        logger.info(f"successfully built all landing tables and snowpipes! \n \n")

def fix_invalidated_input_tables():
    add_missing_tables_to_clarity_sources()
    build_landing_tables()

if __name__ == '__main__':
    fix_invalidated_input_tables()
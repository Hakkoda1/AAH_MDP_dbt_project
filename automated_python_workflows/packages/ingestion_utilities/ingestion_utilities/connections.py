import logging

import snowflake.connector

# Configure the logger for the Snowflake connector
snowflake_logger = logging.getLogger("snowflake.connector")
snowflake_logger.setLevel(logging.WARN)  # Set the Snowflake connector logger level to INFO

def connect_to_snowflake():

    snowflake_connection = snowflake.connector.connect(
        user='CALE.PLISKA@AAH.ORG',
        account='advocate-mdp',
        authenticator='externalbrowser',
        warehouse = 'MDP_DEVELOPMENT_WH',
        database = 'MDP_RAW_DEV',
        schema = 'dbt_cpliska',
        role = 'FR_MDP_DEVELOPER_DEV'
    )
    try:
        return snowflake_connection
    except Exception as e:
        snowflake_logger.error("error occurred while connecting to snowflake {e}")
import re
import io
import contextlib

from dbt.cli.main import dbtRunner, dbtRunnerResult

def execute_dbt_command(command_list) -> (str, dbtRunnerResult):
    captured_output = io.StringIO()
    dbt = dbtRunner()
    with contextlib.redirect_stdout(captured_output):
        response: dbtRunnerResult = dbt.invoke(command_list)
    captured_bytes = captured_output.getvalue().encode('utf-8', errors='ignore')
    captured_txt = captured_bytes.decode('utf-8')

    return captured_txt, response

def generate_dbt_build_command_str(assets: list) -> str:
    """assets can be a list of tables or views"""
    tables = ' '.join([f"+{re.sub(r'^zc_', 'lkp_clr_', asset.lower())}" for asset in assets])
    command = "build --select " + tables
    return command

def generate_dbt_build_command_list(dbt_build_command:str) -> list[str]:
    return dbt_build_command.split()
import asyncio
import shlex
from prefect import get_run_logger

async def run_command(command, stdin=None, non_error_codes=[0]):
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(command),
        stdin=asyncio.subprocess.PIPE if stdin else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate(input=stdin.encode("utf-8") if stdin else None)
    
    if proc.returncode not in non_error_codes:
        logger = get_run_logger()
        logger.error(f"Command '{command}' failed with return code {proc.returncode}")
        logger.error(f"Error output: {stderr.decode('utf-8')}")
        logger.error(f"Standard output: {stdout.decode('utf-8')}")
        raise Exception(f"Command '{command}' failed with return code {proc.returncode}")

    return (stdout, stderr)
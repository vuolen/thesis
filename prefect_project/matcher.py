import os
import asyncio

PATTERN_DIR = os.getenv("PATTERN_DIR")
patterns = os.listdir(PATTERN_DIR)

async def run_command(command, stdin=None):
    proc = await asyncio.create_subprocess_shell(
        command,
        stdin=asyncio.subprocess.PIPE if stdin else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate(input=stdin)

    return (stdout, stderr)

async def ripgrepAll(file, stdin=None):
    if file == "-":
        assert stdin is not None, "stdin must be provided if file is '-'"

    matches = {}
    for pattern in patterns:
        stdout, stderr = await run_command(f"rga -iF --count-matches -f {os.path.join(PATTERN_DIR, pattern)} {file}", stdin=stdin)
        if not stderr == b"":
            raise Exception(f"Error running ripgrep: {stderr}")
        else:
            matches[pattern] = int(stdout) if stdout != b"" else 0
    return matches
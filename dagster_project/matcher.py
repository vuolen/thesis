import os
import asyncio

PATTERN_DIR = os.getenv("PATTERN_DIR")
patterns = os.listdir(PATTERN_DIR)

async def run_command(command):
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    return (stdout, stderr)

async def pdfgrepAll(file, matches = {}):
    matches = {}
    for pattern in patterns:
        stdout, stderr = await run_command(f"pdfgrep -iF --count -f {os.path.join(PATTERN_DIR, pattern)} {file}")
        if not stderr == b"":
            raise Exception(f"Error running pdfgrep: {stderr}")
        else:
            matches[pattern] = int(stdout)
    return matches

async def ripgrepAll(file):
    matches = {}
    for pattern in patterns:
        stdout, stderr = await run_command(f"rg -iF --count-matches -f {os.path.join(PATTERN_DIR, pattern)} {file}")
        if not stderr == b"":
            raise Exception(f"Error running ripgrep: {stderr}")
        else:
            matches[pattern] = int(stdout) if stdout != b"" else 0
    return matches

async def zgrepAll(file):
    matches = {}
    for pattern in patterns:
        stdout, stderr = await run_command(f"zgrep -iF --count -f {os.path.join(PATTERN_DIR, pattern)} {file}")
        if not stderr == b"":
            raise Exception(f"Error running zgrep: {stderr}")
        else:
            matches[pattern] = int(stdout)
    return matches
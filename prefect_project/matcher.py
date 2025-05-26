import os
from prefect_project import command

PATTERN_DIR = os.getenv("PATTERN_DIR")
patterns = os.listdir(PATTERN_DIR)


async def ripgrepAll(file, stdin=None):
    if file == "-":
        assert stdin is not None, "stdin must be provided if file is '-'"

    matches = {}
    for pattern in patterns:
        stdout, stderr = await command.run_command(f"rga -iF --count-matches -f {os.path.join(PATTERN_DIR, pattern)} {file}", stdin=stdin, non_error_codes=[0, 1])
        if not stderr == b"":
            raise Exception(f"Error running ripgrep: {stderr}")
        else:
            matches[pattern] = int(stdout) if stdout != b"" else 0
    return matches
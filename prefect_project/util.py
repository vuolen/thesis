from prefect import task
import aiofiles
import json

@task
async def read_jsonl_from_file(path):
    items = []
    async with aiofiles.open(path, "r") as itemsf:
        async for line in itemsf:
            items.append(json.loads(line.strip()))
    return items
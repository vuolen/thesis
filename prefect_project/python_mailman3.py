from prefect import task
from itertools import groupby

@task
async def to_documents(items):
    threads = groupby(items, key=lambda x: x["scraped_from"]["threadLink"])

    documents = []

    for _,messages in threads.items():
        documents.append({
            "name": messages[0]["name"],
            "files": [
                {
                    "path": message["file"]["path"],
                }
                for message in messages
            ],
        })

    return documents
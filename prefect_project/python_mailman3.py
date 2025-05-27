from prefect import task
from collections import defaultdict

@task
async def to_documents(items):
    threads = defaultdict(list) 

    for item in items:
        threads[item["scraped_from"]["threadLink"]].append(item)

    documents = []
    for threadLink, messages in threads.items(): 
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
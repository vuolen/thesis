from prefect import task, get_run_logger
from collections import defaultdict

@task
async def to_documents(items):
    logger = get_run_logger()
    logger.info(f"Python mailman3: Converting {len(items)} items to documents")
    threads = defaultdict(list) 

    for item in items:
        threads[item["scraped_from"]["threadLink"]].append(item)

    logger.info(f"Python mailman3: Found {len(threads)} threads")

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
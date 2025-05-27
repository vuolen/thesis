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
            "id": messages[0]["id"],
            "name": messages[0]["name"],
            "url": threadLink,
            "file_urls": [message["file_urls"][0] for message in messages],
            "files": [
                {
                    "path": message["files"][0]["path"],
                }
                for message in messages
            ],
        })

    return documents
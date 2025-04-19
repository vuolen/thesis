import os
import json
import asyncio
import atexit
from prefect import task, flow, serve
from prefect.logging import get_run_logger
from prefect.cache_policies import DEFAULT 
from prefect_project import scrapyd_client
from prefect_project.threadparser import parse_threads
from scrapy_project.spiders.cpp_papers import CppPapersSpider
from scrapy_project.spiders.cpp_mailing_lists import CppMailingListsSpider
from scrapy_project.spiders.java_jep import JavaJepSpider
from scrapy_project.spiders.java_specs import JavaSpecsSpider
from scrapy_project.spiders.openjdk_mailman2_mailing_lists import OpenJDKMailman2MailingListsSpider
from scrapy_project.spiders.python_pep import PythonPepSpider
from scrapy_project.spiders.python_discuss import PythonDiscussSpider
from scrapy_project.spiders.python_docs import PythonDocsSpider
from scrapy_project.spiders.python_mailman2_mailing_lists import PythonMailman2MailingListsSpider
from scrapy_project.spiders.python_mailman3_mailing_lists import PythonMailman3MailingListsSpider
from .matcher import ripgrepAll, run_command

FILES_DIR = os.getenv("FILES_DIR")
ITEM_FEEDS_DIR = os.getenv("ITEM_FEEDS_DIR")

@task
async def run_scraper(spider_name: str):
    logger = get_run_logger()
    running_job = None
    try:
        if await scrapyd_client.is_spider_running(spider_name):
            raise Exception(f"Spider {spider_name} is running or pending. Aborting deployment")

        created_job = await scrapyd_client.schedule_spider(spider_name, settings = {
            "FILES_STORE": FILES_DIR,
            "DOWNLOAD_DELAY": 10,
            "HTTPCACHE_ENABLED": False
        })
        running_job = created_job
        logger.info(f"Scheduled job {created_job['jobid']} for spider {spider_name}")

        log_file = None
        items_file = None
        logger.info("Waiting for log and items file to be available")
        while log_file is None or items_file is None:
            jobs = await scrapyd_client.listjobs()
            for job in jobs["running"] + jobs["finished"]:
                if job["id"] == created_job["jobid"]:
                    running_job = job
                    log_file = job.get("log_url", None)
                    items_file = job.get("items_url", None)
            await asyncio.sleep(2)
        log_file = os.path.join(os.getcwd(), log_file.lstrip("/"))
        items_file = os.path.join(os.getcwd(), items_file.replace("/items", ITEM_FEEDS_DIR))
        logger.info(f"Log file available at {log_file}")
        logger.info(f"Items file available at {items_file}")

        while True:
            if os.path.exists(log_file):
                break
            await asyncio.sleep(2)

        with open(log_file, "r") as log:
            while True:
                finished = False
                jobs = await scrapyd_client.listjobs()
                for job in jobs["finished"]:
                    if job["id"] == created_job["jobid"]:
                        finished = True
                        logger.info(f"Spider {spider_name} finished")

                line = log.readline()
                while line:
                    logger.info(line.strip())
                    line = log.readline()

                if finished:
                    break

        logger.info("Spider finished, getting items")

        while True:
            if os.path.exists(items_file):
                break
            await asyncio.sleep(2)

        with open(items_file, "r") as items:
            items = [json.loads(line) for line in items.readlines()]
            return items

    except Exception as e:
        print("run_scraper encountered an exception, killing spider")
        if job:
            id = running_job.get("id", running_job.get("jobid", None))
            if id:
                logger.info(f"Killing job {id}")
                await scrapyd_client.kill_job(id, force=True)
        raise e


@task
def parse_documents(items, parser):
    return parser(items)

@task
async def annotate_documents(documents):
    for document in documents:
        for file in document["files"]:
            try:
                fullPath = os.path.join(FILES_DIR, file["path"])
                document["matches"] = await ripgrepAll(fullPath)
            except Exception as e:
                print(f"Error processing file {file['path']}")
                raise e
    return documents


@task
def save_output(annotated_documents, spider_name):
    output_path = os.path.join(ITEM_FEEDS_DIR, f"{spider_name}-final.jsonl")
    with open(output_path, "w") as outf:
        for document in annotated_documents:
            outf.write(json.dumps(document) + "\n")
    return output_path


@flow(flow_run_name="{spider_name}-collection")
async def collection_flow(spider_name, parser_name):
    output_file = os.path.join(ITEM_FEEDS_DIR, f"{spider_name}.jsonl")

    items = await run_scraper(spider_name)
    documents = parse_documents(items, parsers[parser_name])
    annotated_documents = await annotate_documents(documents)
    save_output(annotated_documents, spider_name)

spiders = [
    (CppPapersSpider.name, "identity"),
    (CppMailingListsSpider.name, "identity"),
    (JavaJepSpider.name, "identity"),
    (JavaSpecsSpider.name, "identity"),
    (OpenJDKMailman2MailingListsSpider.name, "parse_threads"),
    (PythonDiscussSpider.name, "identity"),
    (PythonDocsSpider.name, "identity"),
#    (PythonMailman2MailingListsSpider.name, "parse_threads"),
    (PythonPepSpider.name, "identity"),
#    (PythonMailman3MailingListsSpider.name, "parse_threads"),
]

parsers = {
    "identity": lambda x: x,
    "parse_threads": parse_threads,
}

def main():
    deployments = [
        collection_flow.to_deployment(
            name=f"{spider_name}-collection",
            parameters={"spider_name": spider_name, "parser_name": parser_name},
        )
        for spider_name, parser_name in spiders
    ]
    serve(*deployments)

if __name__ == "__main__":
    asyncio.run(main())
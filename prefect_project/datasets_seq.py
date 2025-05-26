import os
import json
import asyncio
import aiofiles
from prefect import flow, serve, task
from prefect.context import FlowRunContext
from prefect.logging import get_run_logger
from prefect_project import cpp_papers, scrapyd_client
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
from .matcher import ripgrepAll

FILES_DIR = os.getenv("FILES_DIR")
ITEM_FEEDS_DIR = os.getenv("ITEM_FEEDS_DIR")

@task
async def run_scraper(spider_name: str):

    job_id = FlowRunContext.get().flow_run.id
    logger = get_run_logger()

    # with open("/home/lennu/code/thesis/data/feeds/scrapy_project/openjdk-mailman2-mailing-lists/bd1083e7-7520-42e2-bb8b-4c2d1a40344e.jl", "r") as items:
    #     items = [json.loads(line) for line in items.readlines()]
    #     for item in items:
    #         logger.info(f"Yielding item {item['name']}")
    #         yield item

    # return

    if await scrapyd_client.is_spider_running(spider_name):
        raise Exception(f"Spider {spider_name} is running or pending. Aborting deployment")

    created_job = await scrapyd_client.schedule_spider(spider_name, job_id, settings = {
        "FILES_STORE": FILES_DIR,
        "HTTPCACHE_ENABLED": True
    })
    logger.info(f"Scheduled job {created_job['jobid']} for spider {spider_name}")

    full_job_status = None

    while True:
        jobs = await scrapyd_client.listjobs()
        job = jobs.get(created_job["jobid"])
        if job is not None:
            logger.debug(f"Fetched job status {job}")
            full_job_status = job
            if job["status"] == "finished":
                break
        await asyncio.sleep(600)

    return full_job_status

@task
async def log_from_file(file_path):
    logger = get_run_logger()
    async with aiofiles.open(file_path, "r") as logf:
        async for line in logf:
            logger.info(line.strip())

@task
async def read_items_from_file(items):
    items = []
    async with aiofiles.open(items, "r") as itemsf:
        async for line in itemsf:
            items.append(json.loads(line.strip()))
    return items

@task
def parse_items(items, parser_name):
    parser = parsers.get(parser_name, lambda x: [x])
    return [
        document
        for item in items
        for document in parser(item)
    ]

@task
async def annotate_documents(documents):
    logger = get_run_logger()
    annotated_documents = []
    for document in documents:
        document_matches = {}
        for file in document["files"]:
            try:
                if "stdin" in file:
                    file_matches = await ripgrepAll("-", stdin=file["stdin"])
                elif "path" in file:
                    fullPath = os.path.join(FILES_DIR, file["path"])
                    file_matches = await ripgrepAll(fullPath)

                for k,v in file_matches.items():
                    document_matches[k] = document_matches.get(k, 0) + v

            except Exception as e:
                logger.error(f"Error processing file {file.get('path', file.get('stdin'))} from document {document}")
                logger.error(e)
        
        document["matches"] = document_matches

    return annotated_documents
 
@task
def filter_documents(documents):
    return [
        document
        for document in documents
        if sum(document["matches"].values()) > 0
    ]

@task
async def write_documents_to_file(documents, output_file_path):
    async with aiofiles.open(output_file_path, "w") as outf:
        async for document in documents:
            await outf.write(json.dumps(document) + "\n")

async def cleanup_flow(flow, flow_run, state):
    await scrapyd_client.kill_job(flow_run.id, force=True)


@flow(
    flow_run_name="{spider_name}-collection",
    on_failure=[cleanup_flow],
    on_cancellation=[cleanup_flow],
    on_crashed=[cleanup_flow]
)
async def collection_flow(spider_name, parser_name, cached_job=None):

    logger = get_run_logger()
    output_file_path = os.path.join(ITEM_FEEDS_DIR, f"{spider_name}-final.jsonl")

    if cached_job is None:
        finished_job = await run_scraper(spider_name)
        logger.info(f"Job finished: {finished_job}")
        items_file_path = finished_job["items_url"]
        items_file_path = os.path.join(os.getcwd(), items_file_path.replace("/items", ITEM_FEEDS_DIR))

        log_file_path = finished_job["log_url"]
        log_file_path = os.path.join(os.getcwd(), log_file_path.lstrip("/"))
        await log_from_file(log_file_path)
    else:
        items_file_path = f"{ITEM_FEEDS_DIR}/scrapy_project/{spider_name}/{cached_job}.jl"
        logger.info(f"Using cached job {cached_job} for spider {spider_name}")


    items = await read_items_from_file(items_file_path)
    logger.info(f"Read {len(items)} items from file")

    documents = parse_items(items, parser_name)
    logger.info(f"Parsed {len(documents)} documents")

    annotated_documents = await annotate_documents(documents)
    logger.info(f"Annotated {len(annotated_documents)} documents")

    filtered_documents = filter_documents(annotated_documents)
    logger.info(f"Got {len(filtered_documents)} filtered documents")

    await write_documents_to_file(filtered_documents, output_file_path)

spiders = [
    (CppPapersSpider.name, "cpp_papers"),
    (CppMailingListsSpider.name, None),
    (JavaJepSpider.name, None),
    (JavaSpecsSpider.name, None),
    (OpenJDKMailman2MailingListsSpider.name, None),
    (PythonDiscussSpider.name, None),
    (PythonDocsSpider.name, None),
    (PythonMailman2MailingListsSpider.name, None),
    (PythonPepSpider.name, None),
    (PythonMailman3MailingListsSpider.name, None),
]

parsers = {
    "cpp_papers": cpp_papers.to_documents,
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
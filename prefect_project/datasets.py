import os
import json
import asyncio
import time
from prefect import task, flow, serve
from prefect.context import FlowRunContext
from prefect.logging import get_run_logger
from prefect.cache_policies import DEFAULT 
from prefect_project import scrapyd_client
from prefect_project.threadparser import parse_threads
from scrapy_project.items import BaseItem
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
from typing import AsyncGenerator 

FILES_DIR = os.getenv("FILES_DIR")
ITEM_FEEDS_DIR = os.getenv("ITEM_FEEDS_DIR")

@task
async def run_scraper(spider_name: str) -> AsyncGenerator[BaseItem, None]:

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
        "HTTPCACHE_ENABLED": False
    })
    logger.info(f"Scheduled job {created_job['jobid']} for spider {spider_name}")

    log_file_path = None
    log_file = None
    items_file_path = None
    items_file = None

    while True:
        jobs = await scrapyd_client.listjobs()
        for job in jobs["running"] + jobs["finished"]:
            if job["id"] == created_job["jobid"]:
                if log_file_path is None:
                    log_file_path = job.get("log_url", None)
                    if log_file_path is not None:
                        log_file_path = os.path.join(os.getcwd(), log_file_path.lstrip("/"))
                        log_file = open(log_file_path, "r")
                        logger.info(f"Log file available at {log_file_path}")
                if items_file_path is None:
                    items_file_path = job.get("items_url", None)
                    if items_file_path is not None:
                        items_file_path = os.path.join(os.getcwd(), items_file_path.replace("/items", ITEM_FEEDS_DIR))
                        items_file = open(items_file_path, "r")
                        logger.info(f"Items file available at {items_file_path}")

        finished = False
        for job in jobs["finished"]:
            if job["id"] == created_job["jobid"]:
                finished = True
                logger.info(f"Spider {spider_name} finished")

        if log_file is not None:
            line = log_file.readline()
            while line:
                logger.info(line.strip())
                line = log_file.readline()

        if items_file is not None:
            line = items_file.readline()
            while line:
                yield json.loads(line)
                line = items_file.readline()

        if finished:
            break

        await asyncio.sleep(5)

Document = dict
AnnotatedDocument = dict
@task
async def annotate_documents(
        documents: AsyncGenerator[Document, None]
    ) -> AsyncGenerator[AnnotatedDocument, None]:

    async def annotate_document(document: Document) -> AnnotatedDocument:
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
                print(f"Error processing file {file.get('path', file.get('stdin'))} from document {document['id']}")
                raise e

        return {
            **document,
            "matches": document_matches,
        }

    document_queue = asyncio.Queue()

    async def producer():
        async for document in documents:
            await document_queue.put(document)
        await document_queue.put(None)

    asyncio.create_task(producer())


    last_batch_processed = time.time()
    exit = False
    while not exit:
        if time.time() - last_batch_processed > 5 or document_queue.qsize() > 10:
            tasks = []
            for _ in range(min(10, document_queue.qsize())):
                document = await document_queue.get()
                if document is None:
                    exit = True
                    break
                tasks.append(annotate_document(document))

            annotated_documents = await asyncio.gather(*tasks)

            for annotated_document in annotated_documents:
                yield annotated_document

            last_batch_processed = time.time()
        
        await asyncio.sleep(0.1)



@task
async def save_output(
        annotated_documents: AsyncGenerator[AnnotatedDocument, None],
        spider_name: str
    ) -> str:
    output_path = os.path.join(ITEM_FEEDS_DIR, f"{spider_name}-final.jsonl")
    with open(output_path, "w") as outf:
        async for document in annotated_documents:
            outf.write(json.dumps(document) + "\n")
    return output_path

async def cleanup_flow(flow, flow_run, state):
    await scrapyd_client.kill_job(flow_run.id, force=True)


@flow(
    flow_run_name="{spider_name}-collection",
    on_failure=[cleanup_flow],
    on_cancellation=[cleanup_flow],
    on_crashed=[cleanup_flow]
)
async def collection_flow(spider_name, parser_name):
    items = run_scraper(spider_name)
    documents = parsers[parser_name](items) if parser_name is not None else items
    annotated_documents = annotate_documents(documents)
    await save_output(annotated_documents, spider_name)
   

spiders = [
    (CppPapersSpider.name, None),
    (CppMailingListsSpider.name, None),
    (JavaJepSpider.name, None),
    (JavaSpecsSpider.name, None),
    (OpenJDKMailman2MailingListsSpider.name, "parse_threads"),
    (PythonDiscussSpider.name, None),
    (PythonDocsSpider.name, None),
    (PythonMailman2MailingListsSpider.name, "parse_threads"),
    (PythonPepSpider.name, None),
    (PythonMailman3MailingListsSpider.name, "parse_threads"),
]

parsers = {
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
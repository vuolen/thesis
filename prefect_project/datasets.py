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
import aioreactive as rx

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

    return created_job["jobid"]

def create_job_status_subject(jobid):
    logger = get_run_logger()
    subject = rx.AsyncSubject()
    async def task():
        try:
            while True:
                jobs = await scrapyd_client.listjobs()
                job = jobs.get(jobid)
                if job is not None:
                    logger.debug(f"Fetched job status {job}")
                    await subject.asend(job)
                await asyncio.sleep(10)
        except Exception as e:
            await subject.athrow(e)

    asyncio.create_task(task())
    return subject


def create_file_lines_observable(file_path):
    async def agen():
        async with aiofiles.open(file_path, "r") as file:
            while True:
                line = await file.readline()
                while line != "":
                    yield line.strip()
                    line = await file.readline()
                await asyncio.sleep(10)
    return rx.from_async_iterable(agen())

def create_finished_observable(job_status_subject):
    logger = get_run_logger()
    def log_finished(x):
        logger.debug(f"Job finished: {x}")
        return x

    return rx.pipe(
        job_status_subject,
        rx.filter(lambda job: job["status"] == "finished"),
        rx.take(1),
        rx.map(log_finished),
        rx.map(lambda _: True),
        rx.delay(30)
    )

def create_logs_observable(job_status_subject):
    log_lines_stream = rx.pipe(
        job_status_subject,
        rx.filter(lambda job: job.get("log_url") is not None),
        rx.map(lambda job: job["log_url"]),
        rx.map(lambda log_url: os.path.join(os.getcwd(), log_url.lstrip("/"))),
        rx.take(1),
        rx.flat_map(lambda log_url: create_file_lines_observable(log_url))
    )

    finished_stream = create_finished_observable(job_status_subject)
    return rx.pipe(
        log_lines_stream,
        rx.take_until(finished_stream)
    )


def create_items_observable(job_status_stream):
    items_line_stream = rx.pipe(
        job_status_stream,
        rx.filter(lambda job: job.get("items_url") is not None),
        rx.map(lambda job: job["items_url"]),
        rx.map(lambda items_url: os.path.join(os.getcwd(), items_url.replace("/items", ITEM_FEEDS_DIR))),
        rx.take(1),
        rx.flat_map(lambda items_file_path: create_file_lines_observable(items_file_path))
    )

    finished_stream = create_finished_observable(job_status_stream)
    return rx.pipe(
        items_line_stream,
        rx.take_until(finished_stream),
        rx.map(lambda line: json.loads(line)),
    )


Document = dict
AnnotatedDocument = dict
async def annotate_documents(
        document: Document
    ) -> AnnotatedDocument:

    logger = get_run_logger()
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

    return {
        **document,
        "matches": document_matches,
    }
 

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
    parser = parsers.get(parser_name, lambda x: [x])
    output_file_path = os.path.join(ITEM_FEEDS_DIR, f"{spider_name}-final.jsonl")

    if cached_job is None:
        jobid = await run_scraper(spider_name)
        job_status_subject = create_job_status_subject(jobid)
        items_stream = create_items_observable(job_status_subject)
    else:
        items_stream = create_file_lines_observable(f"{ITEM_FEEDS_DIR}/scrapy_project/{spider_name}/{cached_job}.jl")

    documents_to_save = rx.pipe(
        items_stream,
        rx.flat_map(lambda item: rx.from_iterable(parser(item))),
        rx.map_async(lambda document: annotate_documents(document)),
        rx.filter(lambda annotated_document: sum(annotated_document["matches"].values()) > 0),
    )

    logs = create_logs_observable(job_status_subject)
    async def logs_asend(line):
        logger.info(line)
    async def logs_athrow(e):
        logger.error(f"Log stream error: {e}")
    async def logs_aclose():
        logger.info("Log stream completed")
    logs_subscription = await logs.subscribe_async(
        send=logs_asend,
        throw=logs_athrow,
        close=logs_aclose,
    )


    async with aiofiles.open(output_file_path, "w") as outf:
        async for document in rx.to_async_iterable(documents_to_save):
            await outf.write(json.dumps(document) + "\n")
        logger.info("Documents stream completed")

    await logs_subscription.dispose_async()

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
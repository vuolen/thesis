import os
import json
import asyncio
import aiofiles
from prefect import flow, serve, task
from prefect.logging import get_run_logger
from prefect_project import cpp_papers, openjdk_mail, env, scrapy_tasks, util
from scrapy_project.spiders.cpp_papers import CppPapersSpider
from scrapy_project.spiders.cpp_mailing_lists import CppMailingListsSpider
from scrapy_project.spiders.java_jep import JavaJepSpider
from scrapy_project.spiders.java_specs import JavaSpecsSpider
from scrapy_project.spiders.openjdk_mailman2_mailing_lists import OpenJDKMailman2DigestSpider 
from scrapy_project.spiders.python_pep import PythonPepSpider
from scrapy_project.spiders.python_discuss import PythonDiscussSpider
from scrapy_project.spiders.python_docs import PythonDocsSpider
from scrapy_project.spiders.python_mailman3_mailing_lists import PythonMailman3MailingListsSpider
from .matcher import ripgrepAll


@task
async def log_from_file(file_path):
    logger = get_run_logger()
    async with aiofiles.open(file_path, "r") as logf:
        async for line in logf:
            logger.info(line.strip())

@task
async def parse_items(items, parser_name):
    parser = parsers.get(parser_name)
    if parser is None:
        return items
    return await parser(items)

@task
async def annotate_documents(documents):
    logger = get_run_logger()
    for document in documents:
        document_matches = {}
        for file in document["files"]:
            try:
                if "stdin" in file:
                    file_matches = await ripgrepAll("-", stdin=file["stdin"])
                elif "path" in file:
                    fullPath = os.path.join(env.FILES_DIR, file["path"])
                    file_matches = await ripgrepAll(fullPath)

                for k,v in file_matches.items():
                    document_matches[k] = document_matches.get(k, 0) + v

            except Exception as e:
                logger.error(f"Error processing file {file.get('path', file.get('stdin'))} from document {document}")
                logger.error(e)
        
        document["matches"] = document_matches

    return documents
 
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
        for document in documents:
            await outf.write(json.dumps(document) + "\n")


@flow(
    flow_run_name="{spider_name}-collection",
    on_failure=[scrapy_tasks.cleanup_scrapy],
    on_cancellation=[scrapy_tasks.cleanup_scrapy],
    on_crashed=[scrapy_tasks.cleanup_scrapy]
)
async def collection_flow(spider_name, parser_name, cached_job=None):

    logger = get_run_logger()
    output_file_path = os.path.join(env.ITEM_FEEDS_DIR, f"{spider_name}-final.jsonl")

    if cached_job is None:
        finished_job = await scrapy_tasks.run_scraper(spider_name)
        logger.info(f"Job finished: {finished_job}")
        items_file_path = finished_job["items_url"]
        log_file_path = finished_job["log_url"]
        await log_from_file(log_file_path)
    else:
        items_file_path = f"{env.ITEM_FEEDS_DIR}/scrapy_project/{spider_name}/{cached_job}.jl"
        logger.info(f"Using cached job {cached_job} for spider {spider_name}")


    items = await util.read_jsonl_from_file(items_file_path)
    logger.info(f"Read {len(items)} items from file")

    documents = await parse_items(items, parser_name)
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
    (OpenJDKMailman2DigestSpider.name, "openjdk_mailman2_digest"),
    (PythonDiscussSpider.name, None),
    (PythonDocsSpider.name, None),
    (PythonPepSpider.name, None),
    (PythonMailman3MailingListsSpider.name, None),
]

parsers = {
    "cpp_papers": cpp_papers.to_documents,
    "openjdk_mailman2_digest": openjdk_mail.to_documents,
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
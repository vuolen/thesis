import os
import json
import asyncio
from prefect import task, flow, serve
from prefect.logging import get_run_logger
from prefect.cache_policies import DEFAULT 
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


@task(cache_policy=DEFAULT)
async def run_command(cmd):
    logger = get_run_logger()
    logger.info(f"Running command: {cmd}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # 10 MB limit for stdout and stderr
        limit=10 * 1024 * 1024,
    )

    async def handle_stream(stream, lines=None):
        while True:
            try:
                line = await stream.readline()
            except Exception as e:
                logger.error(f"Error reading line: {e}")
                proc.terminate()
            if not line:
                break
            if lines is not None:
                lines.append(line.decode().strip())
            else:
                logger.info(line.decode().strip())

    stdout = []

    await asyncio.gather(
        handle_stream(proc.stdout, lines=stdout),
        handle_stream(proc.stderr),
    )

    returncode = await proc.wait()
    if returncode != 0:
        raise Exception(f"Command failed with return code {returncode}")
    
    return stdout

@task
async def run_scraper(spider_name: str, output_file: str):
    cmd = [
        "scrapy", "crawl", spider_name, "-O", "-:jsonlines",
        "-s", f"FILES_STORE={FILES_DIR}",
        "-s", f"DOWNLOAD_DELAY=1",
        "-s", f"HTTPCACHE_ENABLED=True",
    ]
    output_lines = await run_command(cmd)
    items = []
    for line in output_lines:
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"Error decoding JSON: {line}")
    return items

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


def build_collection_flow(spider, parser):
    output_file = os.path.join(ITEM_FEEDS_DIR, f"{spider.name}.jsonl")

    @flow(name=f"{spider.name}-collection")
    async def build_collection():
        items = await run_scraper(spider.name, output_file)
        documents = parse_documents(items, parser)
        print(documents)
        annotated_documents = await annotate_documents(documents)
        print(annotated_documents)
        save_output(annotated_documents, spider.name)

    return build_collection

@flow
def cpp_papers():
    build_collection_flow(CppPapersSpider, lambda x: x)()

@flow
def cpp_mailing_lists():
    build_collection_flow(CppMailingListsSpider, lambda x: x)()

@flow
def java_jep():
    build_collection_flow(JavaJepSpider, lambda x: x)()

@flow
def java_specs():
    build_collection_flow(JavaSpecsSpider, lambda x: x)()

@flow
def openjdk_mailing_lists():
    build_collection_flow(OpenJDKMailman2MailingListsSpider, parse_threads)()

@flow
def python_discuss():
    build_collection_flow(PythonDiscussSpider, lambda x: x)()

@flow
def python_docs():
    build_collection_flow(PythonDocsSpider, lambda x: x)()

@flow
def python_mailman2_lists():
    build_collection_flow(PythonMailman2MailingListsSpider, parse_threads)()

@flow
def python_pep():
    build_collection_flow(PythonPepSpider, lambda x: x)()

@flow
def python_mailman3_lists():
    build_collection_flow(PythonMailman3MailingListsSpider, parse_threads)()

def main():
    deployments = [
        cpp_papers.to_deployment(name="cpp_papers"),
        cpp_mailing_lists.to_deployment(name="cpp_mailing_lists"),
        java_jep.to_deployment(name="java_jep"),
        java_specs.to_deployment(name="java_specs"),
        openjdk_mailing_lists.to_deployment(name="openjdk_mailing_lists"),
        python_discuss.to_deployment(name="python_discuss"),
        python_docs.to_deployment(name="python_docs"),
        python_mailman2_lists.to_deployment(name="python_mailman2_lists"),
        python_pep.to_deployment(name="python_pep"),
        python_mailman3_lists.to_deployment(name="python_mailman3_lists"),
    ]
    serve(*deployments)

if __name__ == "__main__":
    main()
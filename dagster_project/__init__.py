import dagster as dg
import os
import json
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from dagster_project.threadparser import createThreadParser
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

def build_collection_asset(spider, parser):

    spider_name = spider.name.replace("-", "_")

    @dg.op(name=f"{spider_name}_items")
    def items():
        print("Running spider " + spider.name)
        output_file = os.path.join(ITEM_FEEDS_DIR, f"{spider_name}.jsonl")
        settings = get_project_settings()
        #settings.set("LOG_LEVEL", "ERROR")
        settings.set("FEEDS", {output_file: {"format": "jsonlines", "overwrite": True}})
        settings.set("FILES_STORE", FILES_DIR)
        settings.set("DOWNLOAD_DELAY", 1)
        settings.set("HTTPCACHE_ENABLED", False)
        process = CrawlerProcess(settings=settings)
        process.crawl(spider)
        process.start()

        items = []
        with open(output_file, "r") as outf:
            for line in outf.readlines():
                items.append(json.loads(line))

        return items

    @dg.op(name=f"{spider_name}_parse_documents")
    def parse_documents(items):
        return parser(items) 

    @dg.op(name=f"{spider_name}_include_matches")
    async def include_matches(documents):
        for document in documents:
            for file in document["files"]:
                try:
                    fullPath = os.path.join(FILES_DIR, file["path"])
                    extension = os.path.splitext(file["path"])[1]
                    matches = {}
                    if extension == ".pdf":
                        matches = await pdfgrepAll(fullPath)
                    elif extension == ".octet-stream":
                        matches = await zgrepAll(fullPath)
                    else:
                        matches = await ripgrepAll(fullPath)
                    document["matches"] = matches
                except Exception as e:
                    print(f"Error processing file {file['path']}")
                    raise e
        return documents
    
    @dg.op(name=f"{spider_name}_output_documents")
    def output_documents(documents):
        with open(os.path.join(ITEM_FEEDS_DIR, f"{spider_name}_final.jsonl"), "w") as outf:
            for document in documents:
                outf.write(json.dumps(document) + "\n")

        return documents

    @dg.graph_asset(name=spider_name)
    def collection_asset():
        return output_documents(include_matches(parse_documents(items())))

    return dg.Definitions(assets=[collection_asset])

defs = dg.Definitions.merge(
    build_collection_asset(
        CppPapersSpider,
        lambda x: x,
    ),
    build_collection_asset(
        CppMailingListsSpider,
        lambda x: x,
    ),
    build_collection_asset(
        JavaJepSpider,
        lambda x: x,
    ),
    build_collection_asset(
        JavaSpecsSpider,
        lambda x: x,
    ),
    build_collection_asset(
        OpenJDKMailman2MailingListsSpider,
        createThreadParser("OpenJDKMailman2MailingLists"),
    ),
    build_collection_asset(
        PythonDiscussSpider,
        lambda x: x,
    ),
    build_collection_asset(
        PythonDocsSpider,
        lambda x: x,
    ),
    build_collection_asset(
        PythonMailman2MailingListsSpider,
        createThreadParser("PythonMailman2MailingLists"),
    ),
    build_collection_asset(
        PythonMailman3MailingListsSpider,
        createThreadParser("PythonMailman3MailingLists"),
    ),
    build_collection_asset(
        PythonPepSpider,
        lambda x: x,
    )
)
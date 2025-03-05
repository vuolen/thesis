# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.files import FilesPipeline

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import requests
import re
from urllib.parse import urlparse
import datetime

class ThesisScraperPipeline(FilesPipeline):
    def process_item(self, item, spider):
        item["spider_name"] = spider.name
        item["scraped_at"]= datetime.datetime.now().isoformat()
        return super().process_item(item, spider)

    def file_path(self, request, response=None, info=None, *, item=None):
        parsed = urlparse(request.url)
        sanitize = lambda s: re.sub(r"[^a-zA-Z0-9\-_]", "_", s).strip("_")
        path = f"{item['spider_name']}/{sanitize(parsed.netloc)}/{sanitize(parsed.path)}" + sanitize(parsed.query)
        return path


# class PythonMailman3Pipeline(ThesisScraperPipeline):
#     def media_downloaded(self, response, request, info, *, item):
#         if response.status == 400:
#             raise FileException("Empty mail archive")
#         return super().media_downloaded(response, request, info, item=item)

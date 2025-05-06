# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import hashlib
from scrapy.utils.serialize import ScrapyJSONEncoder 
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from pathlib import Path
from scrapy.utils.python import to_bytes
from typing import cast
import mimetypes

def jsons_item(item):
    encoder = ScrapyJSONEncoder(indent=4)
    return encoder.encode(item)

def hash_item(item):
    objStr = jsons_item(item)
    return hashlib.sha1(objStr.encode()).hexdigest()

class PreProcessPipeline:
    def process_item(self, item, spider):
        item["spider_name"] = spider.name
        item["id"] = hash_item(item["file_urls"])
        item["scraped_at"]= datetime.datetime.now().isoformat()
        return item
    
class DeduplicationPipeline:
    def __init__(self):
        self.seen = {}

    def process_item(self, item, spider):
        self.seen[spider.name] = self.seen.get(spider.name, set())
        if item["id"] in self.seen[spider.name]:
            raise DropItem(f"Duplicate item found: {item['id']}")
        else:
            self.seen[spider.name].add(item["id"])
            return item
        
class CustomFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        path = super().file_path(request, response, info, item=item)

        body = path.split(".")[0]

        if request.url.endswith(".tar.gz"):
            path = f"{body}.tar.gz"
        elif request.url.endswith(".tar.bz2"):
            path = f"{body}.tar.bz2"

        return path
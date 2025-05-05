# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import hashlib
from scrapy.utils.serialize import ScrapyJSONEncoder 
from scrapy.exceptions import DropItem

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
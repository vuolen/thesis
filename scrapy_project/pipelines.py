# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import os
import hashlib
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from scrapy.utils.serialize import ScrapyJSONEncoder 
from scrapy.pipelines.files import FilesPipeline 
from scrapy_project.settings import FILES_STORE

class PreProcessPipeline:
    def process_item(self, item, spider):
        item["scraped_at"]= datetime.datetime.now().isoformat()
        item["spider_name"] = spider.name
        return item
    
class CustomFilesPipeline(FilesPipeline):
    EXPIRES = 999999

    @classmethod
    def split_range(file_url):
        parsed = urlparse(file_url)
        query_dict = parse_qs(parsed.query, strict_parsing=True)

        old_start = datetime.datetime.strptime(query_dict["start"][0], "%Y-%m-%d")
        old_end = datetime.datetime.strptime(query_dict["end"][0], "%Y-%m-%d")
        newInterval = (old_end - old_start) / 2

        return [
            urlunparse(parsed._replace(
                query=urlencode({
                    "start": start, 
                    "end": end, 
                })
            ))
            for start, end in [
                (old_start, old_start + newInterval),
                (old_start + newInterval, old_end)
            ]
        ]

    def item_completed(self, results, item, info):
        if not info.spider.name == "python-mailman3-mailing-lists":
            return super().item_completed(results, item, info)

        item_success = all(x[0] for x in results)
        if item_success:
            return super().item_completed(results, item, info)
        else:
            new_file_urls = []
            for (ok, value), url in zip(results, item["file_urls"]):
                if ok:
                    new_file_urls.append(url)
                else:
                    logging.error(f"Failed to download {url}, splitting range")
                    new = CustomFilesPipeline.split_range(url)
                    logging.info(f"Split {url} into {new}")
                    new_file_urls.extend(new)

            item["file_urls"] = new_file_urls
            return super().process_item(item, info.spider)
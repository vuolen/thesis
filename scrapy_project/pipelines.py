# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import hashlib
import logging
from scrapy.utils.serialize import ScrapyJSONEncoder 
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from scrapy.pipelines.files import FilesPipeline 
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
        item["id"] = hash_item(item)
        item["scraped_at"]= datetime.datetime.now().isoformat()
        return item
    
    
class CustomFilesPipeline(FilesPipeline):
    EXPIRES = 999999

    class RangeTooLargeException(Exception):
        """Custom exception for when the range is too large."""

    @classmethod
    def split_range(self, file_url):
        parsed = urlparse(file_url)
        query_dict = parse_qs(parsed.query, strict_parsing=True)

        old_start = datetime.datetime.strptime(query_dict["start"][0], "%Y-%m-%d")
        old_end = datetime.datetime.strptime(query_dict["end"][0], "%Y-%m-%d")
        newInterval = (old_end - old_start) / 2
        mid = datetime.datetime.strftime(old_start + newInterval, "%Y-%m-%d")

        if mid == query_dict["start"][0] or mid == query_dict["end"][0]:
            return None

        return [
            urlunparse(parsed._replace(
                query=urlencode({
                    "start": start, 
                    "end": end, 
                })
            ))
            for start, end in [
                (query_dict["start"][0], mid),
                (mid, query_dict["end"][0])
            ]
        ]
    
    def media_failed(self, failure, request, info):
        logging.info(f"Media failed, {request.url}, {info.spider.name}")
        if not info.spider.name == "python-mailman3-mailing-lists":
            return super().media_failed(failure, request, info)

        logging.info(failure.value)
        
        if hasattr(failure, "value") and hasattr(failure.value, "response"):
            status = failure.value.response.status
            if status != 403 and status != 404:
                raise self.RangeTooLargeException(f"Range too large for {request.url}")

        return super().media_failed(failure, request, info)

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
                    if value.check(self.RangeTooLargeException):
                        new = CustomFilesPipeline.split_range(url)
                        if new is None:
                            raise DropItem(f"Failed to split {url} from item {item}")
                        logging.info(f"Split {url} into {new}")
                        new_file_urls.extend(new)
                    else:
                        raise DropItem(f"Failed to download {url} from item {item}")

            item["file_urls"] = new_file_urls
            return super().process_item(item, info.spider)
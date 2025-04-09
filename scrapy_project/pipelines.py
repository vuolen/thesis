# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import os
import hashlib
import json
from scrapy.utils.serialize import ScrapyJSONEncoder 
from scrapy.pipelines.files import FilesPipeline, FSFilesStore
from scrapy.exceptions import NotConfigured
from thesis_scraper.settings import FILES_STORE

def jsons_item(item):
    encoder = ScrapyJSONEncoder(indent=4)
    return encoder.encode(item)

def hash_item(item):
    objStr = jsons_item(item)
    return hashlib.sha1(objStr.encode()).hexdigest()

def item_export_path(item):
    return os.path.join(FILES_STORE, item["spider_name"], str(item["id"]), "_item.json")

# class PreProcessPipeline:
#     def process_item(self, item, spider):
#         item["id"] = hash_item(item)
#         item["scraped_at"]= datetime.datetime.now().isoformat()
#         item["spider_name"] = spider.name
#         return item

class DeduplicationPipeline:
    def process_item(self, item, spider):
        return item
        # I dont think this is needed
        """ path = item_export_path(item)
        print(path)
        if os.path.exists(path):
            print("exists")
            with open(path, "rb") as f:
                existing = json.loads(f.read())

                downloaded_urls = [f["url"] for f in existing["files"]] if "files" in existing else []

                if (item["id"] == existing["id"] 
                    and downloaded_urls == existing["file_urls"]
                    and all([os.path.exists(os.path.join(FILES_STORE, file["path"])) for file in existing["files"]])):
                    raise DropItem(f"Item already scraped: {item}")
        return item """

""" # Allow scraped files to be stored with an extension
# and still match when checked for existence
class CustomFileStore(FSFilesStore):
    def stat_file(self, path, info):
        absolute_path = self._get_filesystem_path(path)
        dir_path = os.path.dirname(absolute_path)

        for f in os.listdir(dir_path):
            if f.startswith(os.path.basename(absolute_path)):
                ext = os.path.splitext(f)[1]
                return super().stat_file(path + ext, info)

        return super().stat_file(path, info)

class DownloadFilesPipeline(FilesPipeline):
    EXPIRES = 999999

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = CustomFileStore(args[0])

    def file_path(self, request, response=None, info=None, *, item=None):
        ext = ""
        if response:
            content_type = response.headers.get("Content-Type").decode()
            ext = "." + content_type.split("/")[1].split(";")[0]
        id = hashlib.sha1(request.url.encode()).hexdigest()
        return os.path.join(item["spider_name"], str(item["id"]), id) + ext

class ExportPipeline:
    def process_item(self, item, spider):
        path = item_export_path(item)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(jsons_item(item).encode())

        return item """


# class PythonMailman3Pipeline(ThesisScraperPipeline):
#     def media_downloaded(self, response, request, info, *, item):
#         if response.status == 400:
#             raise FileException("Empty mail archive")
#         return super().media_downloaded(response, request, info, item=item)

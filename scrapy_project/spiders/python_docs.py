import scrapy
from ..items import BaseItem

class PythonDocsSpider(scrapy.Spider):
    name = "python-docs"
    allowed_domains = ["python.org"]
    start_urls = ["https://www.python.org/ftp/python/doc/"]

    def parse(self, response):
        for versionFolderLink in response.css('a[href$="/"]::attr(href)').extract():
            if versionFolderLink != "../":
                yield response.follow(versionFolderLink, self.parseVersionFolder)

    def parseVersionFolder(self, response):
        name = response.url.split("/")[-2]
        links = response.css("a::attr(href)").getall()
        if len(links) > 1:
            links = [link for link in links if "html" in link]
        if len(links) > 1:
            links = [link for link in links if ".tar" in link or ".tgz" in link]
        return BaseItem(
            name=name,
            file_urls=[response.urljoin(links[0])]
        )

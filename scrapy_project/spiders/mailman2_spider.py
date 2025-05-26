import scrapy
import json
from ..items import BaseItem

    
class Mailman2DigestSpider(scrapy.Spider):
    """ Scrapes digests """
    def parse(self, response):
        for listLink in response.css('table tr td a[href^="listinfo"]'):
            listName = listLink.css("::text").get()
            archiveLink = listLink.attrib["href"].replace("listinfo", "/pipermail")
            if self.list_predicate is not None and not self.list_predicate(listName):
                self.logger.info(f"Skipping list {listName} based on predicate")
                continue
            yield response.follow(archiveLink, self.parseList, cb_kwargs=dict(listName=listName))

    def parseList(self, response, listName):
        digestLinks = response.css('a[href$=".txt"]::attr(href)').getall()
        yield BaseItem(
            name=listName,
            file_urls=[response.urljoin(link) for link in digestLinks],
        )

class Mailman2ThreadSpider(scrapy.Spider):
    """ Scrapes individual threads, given data from processing the previous spider """

    def start_requests(self):
        if self.arguments_file is None:
            raise ValueError(f"Arguments file must be set to a valid path, got {self.arguments_file}")

        args = []
        with open(self.arguments_file, "r") as f:
            for line in f:
                args.append(json.loads(line.strip()))

        for arg in args:
            yield scrapy.Request(
                url=arg["url"],
                callback=self.parseThreads,
                cb_kwargs=dict(subjects=arg["subjects"], listName=arg["listName"]),
            )

    def parseThreads(self, response, listName, subjects):
        for thread in response.css('body>ul>li'):
            anchors = thread.css('a[href$=".html"]')
            if len(anchors) == 0:
                continue

            texts = anchors.css("::text").getall()
            if any([text.strip() in subjects for text in texts]):
                links = anchors.css("::attr(href)").getall()
                yield BaseItem(
                    name=listName,
                    file_urls=[response.urljoin(link) for link in links],
                )
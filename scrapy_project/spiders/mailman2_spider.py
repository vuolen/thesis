import scrapy
from ..items import BaseItem

    
class Mailman2Spider(scrapy.Spider):
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

    # def parseThreads(self, response, listName):
    #     for list in response.css('body>ul>li'):
    #         links = list.css('a[href$=".html"]::attr(href)').getall()
    #         if len(links) == 0:
    #             continue

    #         yield BaseItem(
    #             name=listName,
    #             file_urls=[response.urljoin(link) for link in links],
    #         )

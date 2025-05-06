import scrapy
from ..items import BaseItem

    
class Mailman2Spider(scrapy.Spider):
    def parse(self, response):
        for listLink in response.css('table tr td a[href^="listinfo"]'):
            listName = listLink.css("::text").get()
            archiveLink = listLink.attrib["href"].replace("listinfo", "/pipermail")
            yield response.follow(archiveLink, self.parseArchive, cb_kwargs=dict(listName=listName))
            return

    def parseArchive(self, response, listName):
        for byThreadLink in response.css('a[href$="thread.html"]::attr(href)').getall():
            yield response.follow(response.urljoin(byThreadLink), self.parseThreads, cb_kwargs=dict(listName=listName))

    def parseThreads(self, response, listName):
        for list in response.css('body>ul>li'):
            links = list.css('a[href$=".html"]::attr(href)').getall()
            if len(links) == 0:
                continue

            yield BaseItem(
                name=listName,
                file_urls=[response.urljoin(link) for link in links],
            )

import scrapy
from thesis_scraper.items import BaseItem

class Mailman2Spider(scrapy.Spider):

    def parse(self, response):
        for listLink in response.css('table tr td a[href^="listinfo"]::attr(href)'):
                archiveLink = listLink.extract().replace("listinfo", "/pipermail")
                yield response.follow(archiveLink, self.parseArchive)

    def parseArchive(self, response):
        # get  full link
        txtLinks = [response.urljoin(path) for path in response.css('a[href$=".txt"]::attr(href)').extract()]
        return BaseItem(
            name=response.url.split("/")[-2],
            file_urls=txtLinks)
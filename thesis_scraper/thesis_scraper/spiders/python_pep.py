import scrapy
from thesis_scraper.items import BaseItem

class PythonPepSpider(scrapy.Spider):
    name = "python-pep"
    allowed_domains = ["peps.python.org"]
    start_urls = ["https://peps.python.org/api/peps.json"]

    def parse(self, response):
        # get json response
        for k,v in response.json().items():
            yield response.follow(v['url'], self.parsePep)

    def parsePep(self, response):
        # h1 with class "page-title"
        name = response.css('h1.page-title::text').get()
        return BaseItem(
            name=name,
            file_urls=[response.url]
        )

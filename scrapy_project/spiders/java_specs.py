import scrapy
from ..items import BaseItem

class JavaSpecsSpider(scrapy.Spider):
    name = "java-specs"
    allowed_domains = ["docs.oracle.com"]
    start_urls = ["https://docs.oracle.com/javase/specs/"]

    def parse(self, response):
        for pdfLink in response.css('a[href$=".pdf"]'):
            pdfLink = pdfLink.attrib['href']
            yield BaseItem(
                name=pdfLink,
                file_urls=[response.urljoin(pdfLink)]
            )


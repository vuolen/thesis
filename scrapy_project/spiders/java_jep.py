import scrapy
from ..items import BaseItem

class JavaJepSpider(scrapy.Spider):
    name = "java-jep"
    allowed_domains = ["openjdk.org"]
    start_urls = ["https://openjdk.org/jeps/0"]

    def parse(self, response):
        for jepLink in response.css('table.jeps tr td a'):
            name = jepLink.attrib['href'] + " - " + jepLink.css('a::text').get()
            yield BaseItem(
                name=name,
                file_urls=[response.urljoin(jepLink.attrib['href'])]
            )


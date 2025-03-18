import scrapy
from thesis_scraper.items import BaseItem

class CppMailingListsSpider(scrapy.Spider):
    name = "cpp-mailing-lists"
    allowed_domains = ["lists.isocpp.org"]
    start_urls = [
        "https://groups.google.com/a/isocpp.org/g/std-proposals"
    ]

    def parse(self, response):
        BASE_URL = "https://lists.isocpp.org"
        for listName in response.css("td a strong::text").extract():
            yield response.follow(f"{BASE_URL}/{listName.lower()}/", self.parseList)

    def parseList(self, response):
        for link in response.css("a[href$='date.php']"):
            yield response.follow(response.urljoin(link.attrib['href']), self.parsePeriod)

    def parsePeriod(self, response):
        links = response.css(".messages-list a::attr(href)").extract()
        year = response.url.split("/")[-3]
        month = response.url.split("/")[-2]
        yield BaseItem(
            name=f"{year}-{month}",
            file_urls=[response.urljoin(link) for link in links]
        )

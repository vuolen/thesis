import scrapy
from ..items import BaseItem

class CppMailingListsSpider(scrapy.Spider):
    name = "cpp-mailing-lists"
    allowed_domains = ["lists.isocpp.org"]
    start_urls = ["https://lists.isocpp.org/mailman/listinfo.cgi"]

    def parse(self, response):
        BASE_URL = "https://lists.isocpp.org"
        for listName in response.css("td a strong::text").extract():
            yield response.follow(f"{BASE_URL}/{listName.lower()}/", self.parseList)

    def parseList(self, response):
        for link in response.css("a[href$='subject.php']"):
            yield response.follow(response.urljoin(link.attrib['href']), self.parseSubject)

    def parseSubject(self, response):
        topics = response.css(".messages-list h2::text").extract()
        linkLists = response.css(".messages-list ul")
        for topic, linkList in zip(topics, linkLists):
            links = linkList.css("a::attr(href)").extract()
            yield BaseItem(
                name=topic,
                file_urls=[response.urljoin(link) for link in links]
            )

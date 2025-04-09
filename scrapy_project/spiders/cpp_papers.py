import scrapy
from ..items import BaseItem

class CppPapersSpider(scrapy.Spider):
    name = "cpp-papers"
    allowed_domains = ["open-std.org"]
    download_delay = 1
    BASE_URL = "https://www.open-std.org/jtc1/sc22/wg21/docs/papers/"

    def start_requests(self):
        for year in range(1992, 2000):
            yield scrapy.Request(url=f"{self.BASE_URL}{year}/", callback=self.parseStyle1992)
        for year in range(2000, 2002):
            yield scrapy.Request(url=f"{self.BASE_URL}{year}/", callback=self.parseStyle2000)
        for year in range(2002, 2013):
            yield scrapy.Request(url=f"{self.BASE_URL}{year}/", callback=self.parseStyle2002)
        for year in range(2013, 2026):
            yield scrapy.Request(url=f"{self.BASE_URL}{year}/", callback=self.parseStyle2013)

    def parseStyle1992(self, response):
         for listItem in response.css('ul li'):
            links = set(listItem.css("a::attr(href)").extract())

            if len(links) == 0:
                continue

            if len(links) > 1:
                links = [link for link in links if ".ps" not in link]
            if len(links) > 1:
                links = [link for link in links if ".pdf" not in link]
            if len(links) > 1:
                links = [link for link in links if ".asc" not in link]

            link = list(links)[0]

            yield BaseItem(
                name=link,
                file_urls=[response.urljoin(link)],
                scraped_from=response.url
            )
            
            

    def parseStyle2000(self, response):
        # just give up, the formatting is wack
        for link in response.css('a::attr(href)').extract():
            yield BaseItem(
                name=link,
                file_urls=[response.urljoin(link)],
                scraped_from=response.url
            )        

    def parseStyle2002(self, response):
        for tableRow in response.css('tr'):
            link = tableRow.css("a::attr(href)").extract_first()
            name = tableRow.css("td:nth-child(3)::text").extract_first()
            yield BaseItem(
                name=name,
                file_urls=[response.urljoin(link)],
                scraped_from=response.url
            )

    def parseStyle2013(self, response):
        for tableRow in response.css('tr'):
            link = tableRow.css("a::attr(href)").extract_first()
            name = tableRow.css("td:nth-child(2)::text").extract_first()
            yield BaseItem(
                name=name,
                file_urls=[response.urljoin(link)],
                scraped_from=response.url
            )

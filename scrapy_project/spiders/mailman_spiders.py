import scrapy
import datetime
from scrapy.spidermiddlewares.httperror import HttpError
from urllib.parse import urljoin, urlparse
from ..items import BaseItem

    
class Mailman2Spider(scrapy.Spider):
    def parse(self, response):
        priority = -1
        for listLink in response.css('table tr td a[href^="listinfo"]'):
            listName = listLink.css("::text").get()
            archiveLink = listLink.attrib["href"].replace("listinfo", "/pipermail")
            # parse a single list before moving on to the next
            yield response.follow(archiveLink, self.parseArchive, cb_kwargs=dict(listName=listName), priority=priority)
            priority -= 1

    def parseArchive(self, response, listName):
        digest_links = response.css('a[href$=".txt"]::attr(href)').getall()
        yield BaseItem(
            name=listName,
            file_urls=[response.urljoin(link) for link in digest_links],
        )


class Mailman3Spider(scrapy.Spider):

    custom_settings = {
        "DOWNLOAD_DELAY": 60,
        "RETRY_ENABLED": False
    }

    # Use fixed end date instead of today to help http caching
    END_DATE = datetime.date(2026, 1, 1)

    def parse(self, response):
        for tableCell in response.css('table tr td::text'):
            if tableCell.extract().endswith("@python.org"):
                listEmail = tableCell.extract().replace("list", "archive")
                archiveLink = f"https://mail.python.org/archives/list/{listEmail}/export/{listEmail}.mbox.gz"
                yield BaseItem(
                    name=listEmail,
                    file_urls = [f"{archiveLink}?start=1990-01-01&end=2026-01-01"]
                )



        



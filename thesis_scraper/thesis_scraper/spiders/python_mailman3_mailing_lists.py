import scrapy
from thesis_scraper.items import BaseItem

class PythonMailman3MailingListsSpider(scrapy.Spider):
    name = "python-mailman3-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/mailman3/lists/?count=200"]

    # Do not set this to False when running the spider
    # Download these files manually
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "HTTPERROR_ALLOWED_CODES": [400],
    }

    def parse(self, response):
        for tableCell in response.css('table tr td::text'):
            if tableCell.extract().endswith("@python.org"):
                listEmail = tableCell.extract().replace("list", "archive")
                archiveLink = f"https://mail.python.org/archives/list/{listEmail}/export/{listEmail}.mbox.gz"

                yield response.follow(archiveLink, self.parseArchive, method="HEAD")

    def parseArchive(self, response):
        name = response.url.split("/")[-3]
        if response.status == 200:
            yield BaseItem(
                name=name,
                file_urls=[response.url]
            )
        elif response.status == 400:
            file_urls = [response.url + f"?start={i}-01-01&end={i}-12-31" for i in range(1990, 2026)]
            yield BaseItem(
                name=name,
                file_urls=file_urls
            )

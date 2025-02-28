import scrapy

class PythonMailman3MailingListArchivesItem(scrapy.Item):
    name = scrapy.Field()
    file_urls = scrapy.Field()

class PythonMailman3MailingListsSpider(scrapy.Spider):
    name = "python-mailman3-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/mailman3/lists/?count=200"]

    def parse(self, response):
        # td ending with @python.com
        for tableCell in response.css('table tr td::text'):
            if tableCell.extract().endswith("@python.org"):
                listEmail = tableCell.extract().replace("list", "archive")
                archiveLink = f"https://mail.python.org/archives/list{listEmail}/export/{listEmail}.mbox.gz"
                yield {
                    "name": listEmail.split("@")[0],
                    "file_urls": [archiveLink]
                }
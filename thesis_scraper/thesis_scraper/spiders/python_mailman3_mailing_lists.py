import scrapy
from thesis_scraper.spiders.mailman_spiders import Mailman3Spider

class PythonMailman3MailingListsSpider(Mailman3Spider):
    name = "python-mailman3-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/mailman3/lists/?count=200"]

    custom_settings = {
        **(Mailman3Spider.custom_settings or {}),
        # Do not set this to False when running the spider
        "ROBOTSTXT_OBEY": False,
    }

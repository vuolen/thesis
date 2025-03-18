from thesis_scraper.spiders.mailman_spiders import Mailman2Spider

class PythonMailman2MailingListsSpider(Mailman2Spider):
    name = "python-mailman2-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/mailman/listinfo"]
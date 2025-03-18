from thesis_scraper.spiders.mailman_spiders import Mailman2Spider

class OpenJDKMailman2MailingListsSpider(Mailman2Spider):
    name = "openjdk-mailman2-mailing-lists"
    allowed_domains = ["mail.openjdk.org"]
    start_urls = ["https://mail.openjdk.org/mailman/listinfo"]

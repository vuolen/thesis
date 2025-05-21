from .mailman2_spider import Mailman2Spider

class OpenJDKMailman2MailingListsSpider(Mailman2Spider):
    name = "openjdk-mailman2-mailing-lists"
    allowed_domains = ["mail.openjdk.org"]
    start_urls = ["https://mail.openjdk.org/mailman/listinfo"]
    download_delay = 1

    def list_predicate(self, listName):
        if listName.endswith("changes"):
            return False
        return True
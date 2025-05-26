from .mailman2_spider import Mailman2DigestSpider, Mailman2ThreadSpider

class OpenJDKMailman2DigestSpider(Mailman2DigestSpider):
    name = "openjdk-mailman2-mailing-lists"
    allowed_domains = ["mail.openjdk.org"]
    start_urls = ["https://mail.openjdk.org/mailman/listinfo"]
    download_delay = 1

    def list_predicate(self, listName):
        if listName.lower().endswith("changes"):
            return False
        return True
    
class OpenJDKMailman2ThreadSpider(Mailman2ThreadSpider):
    name = "openjdk-mailman2-thread"
    allowed_domains = ["mail.openjdk.org"]
    download_delay = 1
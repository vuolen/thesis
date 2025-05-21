from .mailman2_spider import Mailman2Spider

class PythonMailman2MailingListsSpider(Mailman2Spider):
    name = "python-mailman2-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/mailman/listinfo"]

    def list_predicate(self, listName):
        if listName == "python-list":
            return False
        return True
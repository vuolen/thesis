import scrapy
import urllib
from scrapy_project.items import BaseItem
from common.patterns import PATTERNS

class PythonMailman3MailingListsSpider(scrapy.Spider):
    name = "python-mailman3-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/archives/?count=200"]

    custom_settings = {
        "DOWNLOAD_DELAY": 5,
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_IGNORE_HTTP_CODES": [500, 502],
    }

    def list_predicate(self, listName):
        lowered = listName.lower()
        if lowered.endswith("changes") or lowered.endswith("commits") or lowered.endswith("checkins"):
            return False
        return True

    # email list -> search -> page -> message -> thread

    def parse(self, response):
        for mlist in response.css('span.list-address::text').getall():
            mlist = mlist.strip()
            if not self.list_predicate(mlist):
                self.logger.info(f"Skipping list {mlist} based on predicate")
                continue
            for feature, patterns in PATTERNS.items():
                for pattern in patterns:
                    qstr = urllib.parse.urlencode({
                        "mlist": mlist,
                        "q": f'"{pattern}"',
                    })
                    yield response.follow(
                        f"https://mail.python.org/archives/search?{qstr}",
                        callback=self.parse_search,
                        cb_kwargs=dict(mlist=mlist, feature=feature, pattern=pattern)
                    )

    def parse_search(self, response, mlist, feature, pattern):
        pages = [
            int(pageText) 
            for pageText in response.css('a.page-link::text').getall() 
            if pageText.isdigit()
        ]
        if len(pages) == 0:
            yield response.follow(
                response.url,
                callback=self.parse_search_page,
                cb_kwargs=dict(mlist=mlist, feature=feature, pattern=pattern, page=1),
            )
        else:
            last_page = max(pages)

            for page in range(1, last_page + 1):
                qstr = urllib.parse.urlencode({
                    "mlist": mlist,
                    "q": f'"{pattern}"',
                    "page": page,
                })
                yield response.follow(
                    response.urljoin(f"?{qstr}"),
                    callback=self.parse_search_page,
                    cb_kwargs=dict(mlist=mlist, feature=feature, pattern=pattern, page=page),
                )

    def parse_search_page(self, response, mlist, feature, pattern, page):
        for messageAnchor in response.css('span.thread-title a'):
            yield response.follow(
                messageAnchor.attrib["href"],
                callback=self.parse_message,
                cb_kwargs=dict(mlist=mlist, feature=feature, pattern=pattern, page=page),
            )

    def parse_message(self, response, mlist, feature, pattern, page):
        threadName = response.css('h1::text').get()
        for anchor in response.css('a'):
            text = anchor.css("span::text").get()
            if text != None and "Back to the thread" in text:
                threadLink = anchor.attrib["href"]
                threadLink = threadLink.split("#")[0]
                yield BaseItem(
                    name=threadName,
                    file_urls=[response.urljoin(threadLink)],
                    scraped_from={
                        "mlist": mlist,
                        "feature": feature,
                        "pattern": pattern,
                        "page": page,
                    }
                )

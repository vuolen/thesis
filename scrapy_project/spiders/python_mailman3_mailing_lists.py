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
        "HTTPCACHE_IGNORE_HTTP_CODES": [500, 502, 503],
    }

    def list_predicate(self, listName):
        lowered = listName.lower().split("@")[0]
        if lowered.endswith("changes") or lowered.endswith("commits") or lowered.endswith("checkins") or lowered == "python-list":
            return False
        return True

    # email list -> search -> page -> message -> thread

    def parse(self, response):
        for mlist in response.css('span.list-address::text').getall():
            mlist = mlist.strip()
            if not self.list_predicate(mlist):
                self.logger.info(f"Skipping list {mlist} based on predicate")
                continue
            for feature in PATTERNS:
                boolean_q = " OR ".join([f'"{pattern}"' for pattern in PATTERNS[feature]])
                qstr = urllib.parse.urlencode({
                    "mlist": mlist,
                    "q": boolean_q,
                })
                yield response.follow(
                    f"https://mail.python.org/archives/search?{qstr}",
                    callback=self.parse_search,
                    cb_kwargs=dict(mlist=mlist, feature=feature, search=boolean_q)
                )

    def parse_search(self, response, mlist, feature, search):
        pages = [
            int(pageText) 
            for pageText in response.css('a.page-link::text').getall() 
            if pageText.isdigit()
        ]
        if len(pages) == 0:
            yield response.follow(
                response.url,
                callback=self.parse_search_page,
                cb_kwargs=dict(mlist=mlist, feature=feature, search=search, page=1),
            )
        else:
            last_page = max(pages)

            pages_to_scrape = last_page + last_page * 10
            self.logger.info(f"Scraping {pages_to_scrape} pages for {mlist} with feature {feature} and search {search}, takes around {pages_to_scrape * 5 // 60} minutes")

            for page in range(1, last_page + 1):
                qstr = urllib.parse.urlencode({
                    "mlist": mlist,
                    "q": search,
                    "page": page,
                })
                yield response.follow(
                    response.urljoin(f"?{qstr}"),
                    callback=self.parse_search_page,
                    cb_kwargs=dict(mlist=mlist, feature=feature, search=search, page=page),
                )

    def parse_search_page(self, response, mlist, feature, search, page):
        for messageAnchor in response.css('span.thread-title a'):
            yield response.follow(
                messageAnchor.attrib["href"],
                callback=self.parse_message,
                cb_kwargs=dict(mlist=mlist, feature=feature, search=search, page=page),
            )

    def parse_message(self, response, mlist, feature, search, page):
        threadName = response.css('h1::text').get()
        threadLink = None
        for anchor in response.css('a'):
            text = anchor.css("span::text").get()
            if text != None and "Back to the thread" in text:
                threadLink = anchor.attrib["href"]
                threadLink = threadLink.split("#")[0]
                break

        yield BaseItem(
            name=threadName,
            file_urls=[response.url],
            scraped_from={
                "threadLink": threadLink,
                "mlist": mlist,
                "feature": feature,
                "search": search,
                "page": page,
            }
        )

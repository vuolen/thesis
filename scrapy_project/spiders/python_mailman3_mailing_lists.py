import scrapy

from scrapy_project.items import BaseItem

class PythonMailman3MailingListsSpider(scrapy.Spider):
    name = "python-mailman3-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/archives/?count=200"]

    custom_settings = {
        "DOWNLOAD_DELAY": 10,
    }

    def parse(self, response):
        for listAddress in response.css('span.list-address::text').getall():
            archiveLink = f"https://mail.python.org/archives/list/{listAddress}/latest"
            cb_kwargs = dict(listAddress=listAddress)
            yield scrapy.Request(archiveLink, callback=self.parse_list, cb_kwargs=cb_kwargs)

    def parse_list(self, response, listAddress):
        page_links = response.css('a.page-link::attr(href)').getall()
        if not page_links:
            yield response.follow(
                response.url,
                callback=self.parse_list_page,
                cb_kwargs=dict(listAddress=listAddress)
            )
        else:
            last_page = max([
                int(pageText) 
                for pageText in page_links
                if pageText.isdigit()
            ])
            for page in range(1, last_page + 1):
                yield response.follow(
                    f"?page={page}",
                    callback=self.parse_list_page,
                    cb_kwargs=dict(listAddress=listAddress)
                )

    def parse_list_page(self, response, listAddress):
        for threadAnchor in response.css('a.thread-title').getall():
            link = response.urljoin(threadAnchor.attrib["href"])
            name = threadAnchor.css("::text").get()
            yield BaseItem(
                name=name,
                file_urls = [link]
            )
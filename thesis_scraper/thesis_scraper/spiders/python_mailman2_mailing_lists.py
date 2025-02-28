import scrapy

class PythonMailman2MailingListArchivesItem(scrapy.Item):
    name = scrapy.Field()
    file_urls = scrapy.Field()

class PythonMailman2MailingListsSpider(scrapy.Spider):
    name = "python-mailman2-mailing-lists"
    allowed_domains = ["mail.python.org"]
    start_urls = ["https://mail.python.org/mailman/listinfo"]


    def parse(self, response):
        for listLink in response.css('table tr td a[href^="listinfo"]::attr(href)'):
                archiveLink = listLink.extract().replace("listinfo", "/pipermail")
                yield response.follow(archiveLink, self.parseArchive)

    def parseArchive(self, response):
        # get  full link
        txtLinks = [response.urljoin(path) for path in response.css('a[href$=".txt"]::attr(href)').extract()]
        return PythonMailman2MailingListArchivesItem(
            name=response.url.split("/")[-2],
            file_urls=txtLinks)
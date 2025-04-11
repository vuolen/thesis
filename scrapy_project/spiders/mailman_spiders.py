import scrapy
import datetime
from scrapy.spidermiddlewares.httperror import HttpError
from urllib.parse import urljoin, urlparse
from ..items import BaseItem

    
class Mailman2Spider(scrapy.Spider):
    def parse(self, response):
        priority = -1
        for listLink in response.css('table tr td a[href^="listinfo"]'):
            listName = listLink.css("::text").get()
            archiveLink = listLink.attrib["href"].replace("listinfo", "/pipermail")
            # parse a single list before moving on to the next
            yield response.follow(archiveLink, self.parseArchive, cb_kwargs=dict(listName=listName), priority=priority)
            priority -= 1

    def parseArchive(self, response, listName):
        digest_links = response.css('a[href$=".txt"]::attr(href)').getall()
        yield BaseItem(
            name=listName,
            file_urls=[response.urljoin(link) for link in digest_links],
        )


class Mailman3Spider(scrapy.Spider):

    custom_settings = {
        "DOWNLOAD_DELAY": 60,
        "RETRY_ENABLED": False
    }

    # Use fixed end date instead of today to help http caching
    END_DATE = datetime.date(2026, 1, 1)

    @staticmethod
    def replaceNextRequests(request, nextRequests):
        return request.replace(cb_kwargs={
            **request.cb_kwargs,
            "next_requests": nextRequests
        })
    
    @staticmethod
    def popNextRequest(nextRequests):
        req = nextRequests[0]
        return Mailman3Spider.replaceNextRequests(req, nextRequests[1:])

    def parse(self, response):
        for tableCell in response.css('table tr td::text'):
            if tableCell.extract().endswith("@python.org"):
                listEmail = tableCell.extract().replace("list", "archive")
                archiveLink = f"https://mail.python.org/archives/list/{listEmail}/export/{listEmail}.mbox.gz"
                range = (datetime.date(1990, 1, 1), self.END_DATE)
                cb_kwargs = {
                    "listName": listEmail,
                    "range": range,
                    "next_requests": []
                }
                yield response.follow(archiveLink, self.parseRange, cb_kwargs=cb_kwargs, errback=self.onError)

    def parseRange(self, response, listName, range, next_requests):
        latency = response.meta.get("download_latency", None)
        self.logger.info(f"Downloaded {listName} from {range[0]} to {range[1]} in {latency} seconds")
        self.logger.info(f"{len(next_requests)} requests left for {listName}")

        if len(next_requests) > 0:
            yield Mailman3Spider.popNextRequest(next_requests)

    def onError(self, failure):
        latency = failure.request.meta.get("download_latency", None)
        if failure.check(HttpError):
            if failure.value.response.status != 400:
                return

        start = failure.request.cb_kwargs["range"][0]
        end = failure.request.cb_kwargs["range"][1]
        newInterval = (end - start) / 2

        self.logger.info("".join([
            f"Failed to download {failure.request.url} ",
            f"({latency} seconds)" if latency else "(cached), ",
            "splitting into two"
        ]))

        base_url = urljoin(failure.request.url, urlparse(failure.request.url).path)


        secondHalf = failure.request.replace(
            url = base_url + f"?start={(start + newInterval).strftime('%Y-%m-%d')}&end={end.strftime('%Y-%m-%d')}",
            cb_kwargs = {**failure.request.cb_kwargs, "range": (start + newInterval, end)}
        )

        firstHalf = failure.request.replace(
            url = base_url + f"?start={start.strftime('%Y-%m-%d')}&end={(start + newInterval).strftime('%Y-%m-%d')}",
            cb_kwargs = {**failure.request.cb_kwargs, "range": (start, start + newInterval)}
        )

        nextRequests = [firstHalf, secondHalf] + failure.request.cb_kwargs["next_requests"]

        yield Mailman3Spider.popNextRequest(nextRequests)

        



import scrapy
import os
import datetime
import gzip
import mailbox
import tempfile
import email
from scrapy.spidermiddlewares.httperror import HttpError
from urllib.parse import urljoin, urlparse
from thesis_scraper.items import BaseItem
from email.utils import parsedate_to_datetime
from disjoint_set import DisjointSet

# working dir 
TMP_DIR = ".tmp"
os.makedirs(TMP_DIR, exist_ok=True)

class MailmanSpider(scrapy.Spider):

    @staticmethod
    def emptyState(listName):
        return {
            "listName": listName,
            "messages": {},
            "threads": None,
            "digest_counter": 0,
            "total_digests": 0
        }
    
    
    def parseDigestFile(self, response, state, **kwargs):
        data = response.body
        if response.headers.get("Content-Type") == b"application/gzip":
            data = gzip.decompress(data)

        with tempfile.NamedTemporaryFile("wb", delete_on_close=False, dir=TMP_DIR) as tmp:
            tmp.write(data.decode("ascii", "backslashreplace").encode("ascii", "backslashreplace"))
            tmp.close()
            mbox = mailbox.mbox(tmp.name)
            for key in mbox.iterkeys():
                try:
                    msg = mbox[key]
                except email.errors.MessageParseError:
                    self.logger.error(f"Failed to parse message {key} in {response.url} for {state['listName']}")
                    continue
                state["messages"][str(msg["Message-ID"])] = msg
        

    def yieldThreads(self, state):

        state["threads"] = DisjointSet.from_iterable(state["messages"].keys())

        for msgId, msg in state["messages"].items():
            linkedIds = str(msg.get("In-Reply-To", "")).split()
            linkedIds = [msgId for msgId in linkedIds if msgId in state["messages"]]

            for linkedId in linkedIds:
                state["threads"].union(linkedId, msgId)

        for thread in list(state["threads"]):
            msgs = [state["messages"][msgId] for msgId in thread if msgId in state["messages"]]

            def dateKey(msg):
                msgDate = parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else datetime.datetime.max
                if msgDate.tzinfo is None or msgDate.tzinfo.utcoffset(msgDate) is None:
                    msgDate = msgDate.replace(tzinfo=datetime.timezone.utc)
                return msgDate

            msgs.sort(key=dateKey)

            yield BaseItem(
                name=str(msgs[0]["Subject"]),
                payload={
                    "listName": state["listName"],
                    "msgs": [msg.as_string() for msg in msgs],
                    "total_digests": state["total_digests"]
                }
            )


class Mailman2Spider(MailmanSpider):
    def parse(self, response):
        priority = -1
        for listLink in response.css('table tr td a[href^="listinfo"]'):
            listName = listLink.css("::text").get()
            archiveLink = listLink.attrib["href"].replace("listinfo", "/pipermail")
            # parse a single list before moving on to the next
            yield response.follow(archiveLink, self.parseArchive, cb_kwargs=dict(listName=listName), priority=priority)
            priority -= 1

    def parseArchive(self, response, listName):
        state = self.emptyState(listName)
        digest_links = response.css('a[href$=".txt"]')
        state["total_digests"] = len(digest_links)
        for link in digest_links:
            yield response.follow(link.attrib['href'], self.parseDigest, cb_kwargs=dict(state=state))

    def parseDigest(self, response, state):
        self.parseDigestFile(response, state)
        state["digest_counter"] += 1
        if state["digest_counter"] == state["total_digests"]:
            yield from self.yieldThreads(state)
        


class Mailman3Spider(MailmanSpider):

    custom_settings = {
        "DOWNLOAD_DELAY": 60,
        "RETRY_ENABLED": False
    }

    # Use fixed end date instead of today to help http caching
    END_DATE = datetime.date(2026, 1, 1)

    @staticmethod
    def emptyState(listName):
        superState = MailmanSpider.emptyState(listName)
        return {
            **superState,
            "next_requests": []
        }

    @staticmethod
    def replaceNextRequests(request, nextRequests):
        return request.replace(cb_kwargs={
            **request.cb_kwargs,
            "state": {
                **request.cb_kwargs["state"],
                "next_requests": nextRequests
            }
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
                state = self.emptyState(listEmail)
                range = (datetime.date(1990, 1, 1), self.END_DATE)
                yield response.follow(archiveLink, self.parseRange, cb_kwargs=dict(listName=listEmail, state=state, range=range), errback=self.onError)

    def parseRange(self, response, listName, state, range):
        latency = response.meta.get("download_latency", None)
        self.logger.info(f"Downloaded {listName} from {range[0]} to {range[1]} in {latency} seconds")
        self.parseDigestFile(response, state)

        self.logger.info(f"{len(state["next_requests"])} requests left for {listName}")

        if len(state["next_requests"]) > 0:
            yield Mailman3Spider.popNextRequest(state["next_requests"])
        else:
            yield from self.yieldThreads(state)


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

        nextRequests = [firstHalf, secondHalf] + failure.request.cb_kwargs["state"]["next_requests"]

        yield Mailman3Spider.popNextRequest(nextRequests)

        



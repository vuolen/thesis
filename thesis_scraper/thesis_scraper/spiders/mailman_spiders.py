import scrapy
import datetime
import gzip
import numpy as np
from thesis_scraper.items import BaseItem
from email import message_from_bytes
from email.utils import parsedate_to_datetime
from disjoint_set import DisjointSet

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
    
    
    def parseDigestFile(self, response, state):
        data = response.body
        if response.headers.get("Content-Type") == b"application/gzip":
            data = gzip.decompress(data)

        for message in data.split(b"From "):
            msg = message_from_bytes(b"From " + message)
            state["messages"][msg["Message-ID"]] = msg

        
        state["digest_counter"] += 1
        if state["digest_counter"] == state["total_digests"]:
            yield from self.yieldThreads(state)

    def yieldThreads(self, state):

        state["threads"] = DisjointSet.from_iterable(state["messages"].keys())

        for msgId, msg in state["messages"].items():
            linkedIds = msg.get("In-Reply-To", "").split()
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
        priority = 0
        for listLink in response.css('table tr td a[href^="listinfo"]'):
            listName = listLink.css("::text").get()
            archiveLink = listLink.attrib["href"].replace("listinfo", "/pipermail")
            # parse a single list before moving on to the next
            yield response.follow(archiveLink, self.parseArchive, cb_kwargs=dict(listName=listName), priority=priority)
            priority += 1

    def parseArchive(self, response, listName):
        state = self.emptyState(listName)
        digest_links = response.css('a[href$=".txt"]')
        state["total_digests"] = len(digest_links)
        for link in digest_links:
            yield response.follow(link.attrib['href'], self.parseDigestFile, cb_kwargs=dict(state=state))
        


class Mailman3Spider(MailmanSpider):

    custom_settings = {
        "HTTPERROR_ALLOWED_CODES": [400],
    }

    def parse(self, response):
        for tableCell in response.css('table tr td::text'):
            if tableCell.extract().endswith("@python.org"):
                listEmail = tableCell.extract().replace("list", "archive")
                archiveLink = f"https://mail.python.org/archives/list/{listEmail}/export/{listEmail}.mbox.gz"
                yield response.follow(archiveLink, self.parseArchive, method="HEAD", cb_kwargs=dict(listName=listEmail))

    def parseArchive(self, response, listName):
        state = self.emptyState(listName)
        if response.status == 200:
            state["total_digests"] = 1
            yield response.follow(response.url, self.parseDigestFile, cb_kwargs=dict(state=state))
        elif response.status == 400:
            urls = [response.url + f"?start={i}-01-01&end={i}-12-31" for i in range(1990, 2026)]
            state["total_digests"] = len(urls)
            for url in urls:
                yield response.follow(url, self.parseDigestFile, cb_kwargs=dict(state=state))
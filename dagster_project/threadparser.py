import mailbox
import email
import datetime
import dagster as dg
import os
from disjoint_set import DisjointSet
from email.utils import parsedate_to_datetime

FILES_DIR = os.getenv("FILES_DIR")

def createThreadParser(name):
    @dg.op(name=f"{name}_read_messages")
    def read_messages(item):
        messages = {}
        mailboxFiles = [os.path.join(FILES_DIR, file["path"]) for file in item["files"]]
        for file in mailboxFiles:
            mbox = mailbox.mbox(file)
            for key in mbox.iterkeys():
                try:
                    msg = mbox[key]
                    messages[msg["Message-ID"]] = msg
                except email.errors.MessageParseError:
                    print(f"Failed to parse message {key} in {file}")
                    continue
        return messages

    @dg.op(name=f"{name}_build_threads")
    def build_threads(messages):
        threads = DisjointSet.from_iterable(messages.keys())

        for msgId, msg in messages.items():
            linkedIds = str(msg.get("In-Reply-To", "")).split() + str(msg.get("References", "")).split()
            linkedIds = [msgId for msgId in linkedIds if msgId in messages]
            for linkedId in linkedIds:
                threads.union(linkedId, msgId)

        return messages, threads

    @dg.op(name=f"{name}_sort_threads")
    def sort_threads(messages, threads):
        for thread in list(threads):
            msgs = [messages[msgId] for msgId in thread if msgId in messages]

            def dateKey(msg):
                msgDate = parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else datetime.datetime.max
                if msgDate.tzinfo is None or msgDate.tzinfo.utcoffset(msgDate) is None:
                    msgDate = msgDate.replace(tzinfo=datetime.timezone.utc)
                return msgDate

            msgs.sort(key=dateKey)

    @dg.op(name=f"{name}_parse_threads")
    def parse_threads(mboxFiles):
        return sort_threads(*build_threads(read_messages(mboxFiles)))

    return parse_threads
         


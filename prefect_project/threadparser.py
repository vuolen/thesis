import mailbox
import email
import datetime
import os
from disjoint_set import DisjointSet
from email.utils import parsedate_to_datetime
from prefect import task, flow
import sys

FILES_DIR = os.getenv("FILES_DIR")


@task
def read_messages(item):
    """Read messages from mailbox files and return them as a dictionary."""
    messages = {}
    mailboxFiles = [os.path.join(FILES_DIR, file["path"]) for file in item["files"]]
    
    for file in mailboxFiles:
        mbox = mailbox.mbox(file)
        for key in mbox.iterkeys():
            try:
                msg = mbox[key]
                messages[msg["Message-ID"]] = msg
            except email.errors.MessageParseError:
                print(f"Failed to parse message {key} in {file}", file=sys.stderr)
                continue
    return messages


@task
def build_thread_groups(messages):
    threads = DisjointSet.from_iterable(messages.keys())

    for msgId, msg in messages.items():
        linkedIds = str(msg.get("In-Reply-To", "")).split() + str(msg.get("References", "")).split()
        linkedIds = [msgId for msgId in linkedIds if msgId in messages]
        for linkedId in linkedIds:
            threads.union(linkedId, msgId)

    return threads


@task
def get_threads(messages, threads):
    grouped_by_thread = []
    for thread in list(threads):
        msgs = [messages[msgId] for msgId in thread if msgId in messages]

        def dateKey(msg):
            msgDate = parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else datetime.datetime.max
            if msgDate.tzinfo is None or msgDate.tzinfo.utcoffset(msgDate) is None:
                msgDate = msgDate.replace(tzinfo=datetime.timezone.utc)
            return msgDate

        msgs.sort(key=dateKey)
        grouped_by_thread.append(msgs)

    return grouped_by_thread


@task
def parse_threads(items):
    """Parse mailbox files, build threads, and sort them."""
    documents = []
    for item in items:
        messages = read_messages(items)
        thread_groups = build_thread_groups(messages)
        threads = get_threads(messages, thread_groups)
        for thread in threads:
            documents.append(thread)
        


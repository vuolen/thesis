import mailbox
import email
import datetime
import os
from disjoint_set import DisjointSet
from email.utils import parsedate_to_datetime
from prefect import get_run_logger, task
from prefect.artifacts import (
    create_progress_artifact,
    update_progress_artifact,
)
import sys

FILES_DIR = os.getenv("FILES_DIR")

def read_messages(item):
    logger = get_run_logger()
    messages = {}
    mailboxFiles = [os.path.join(FILES_DIR, file["path"]) for file in item["files"]]
    
    for file in mailboxFiles:
        mbox = mailbox.mbox(file)
        for key in mbox.iterkeys():
            try:
                msg = mbox[key]
                messages[msg["Message-ID"]] = msg
            except email.errors.MessageParseError:
                logger.error(f"Failed to parse message {key} in {file}")
                continue
    return messages

def build_thread_groups(messages):
    threads = DisjointSet.from_iterable(messages.keys())

    for msgId, msg in messages.items():
        linkedIds = str(msg.get("In-Reply-To", "")).split() + str(msg.get("References", "")).split()
        linkedIds = [msgId for msgId in linkedIds if msgId in messages]
        for linkedId in linkedIds:
            threads.union(linkedId, msgId)

    return list(threads.itersets())

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
    logger = get_run_logger()
    # An item is a mailing list and all it's digests
    progress_artifact_id = create_progress_artifact(
        progress=0.0,
    )
    progress_step = 1 / len(items)
    os.makedirs(os.path.join(FILES_DIR, "threads"), exist_ok=True)
    documents = []
    for i,item in enumerate(items):
        messages = read_messages(item)
        logger.info(f"Read {len(messages)} messages from {item['name']}")
        thread_groups = build_thread_groups(messages)
        logger.info(f"Built {len(thread_groups)} thread groups for {item['name']}")
        threads = get_threads(messages, thread_groups)
        logger.info(f"Got {len(threads)} threads for {item['name']}")
        
        for index, thread in enumerate(threads):
            thread_file_path = f"threads/{item["id"]}-{index}.jl"
            with open(os.path.join(FILES_DIR, thread_file_path), "w") as thread_file:
                for msg in thread:
                    thread_file.write(f"{msg}\n")

            documents.append({
                "name": thread[0]["Subject"],
                "list": item["name"],
                "id": f"{item["id"]}-{index}",
                "scraped_at": item["scraped_at"],
                "files": [{"path": thread_file_path}],
            })

        update_progress_artifact(
            artifact_id=progress_artifact_id,
            progress=progress_step * (i + 1),
        )

    return documents
            
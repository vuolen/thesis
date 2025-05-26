import json
import os
import aiofiles
from prefect import get_run_logger, task 
from common.patterns import PATTERNS
from prefect_project import env, scrapy_tasks, util, command
from prefect.context import FlowRunContext

"""

The first scraper scrapes full digests by month, because it's faster.
Then messages that contain the patterns are identified and their subjects are extracted.
The second scraper then scrapes the threads of those messages.

"""

@task
async def filter_openjdk_digests(digest_items):
    logger = get_run_logger()
    thread_scraper_args = []
    for item in digest_items:
        list_name = item["name"]
        logger.debug(f"Processing list {list_name}")
        for file in item["files"]:
            absolute_path = os.path.join(env.FILES_DIR, file["path"])
            threads_url = file["url"].replace(".txt", "/thread.html")
            digest_subjects = []

            for _, keywords in PATTERNS.items():
                for keyword in keywords:
                    cmd = f"mboxgrep -p 'grep ^Subject:' -m mbox -E '{keyword}' {absolute_path}"
                    (stdout, stderr) = await command.run_command(cmd)
                    if not stderr == b"":
                        logger.error(stderr)
                        raise Exception(f"Error running {cmd}")
                    keyword_subjects = [subject.decode("utf-8").replace("Subject: ", "", 1).strip() for subject in stdout.splitlines()]
                    keyword_subjects = set(keyword_subjects)
                    digest_subjects.extend(keyword_subjects)

            if len(digest_subjects) > 0:
                thread_scraper_args.append({
                    "url": threads_url,
                    "subjects": list(digest_subjects),
                    "listName": list_name,
                })
    return thread_scraper_args

@task
async def to_documents(items):
    thread_scraper_args = await filter_openjdk_digests(items)
    args_file_path = os.path.join(env.ITEM_FEEDS_DIR, "openjdk-mailman2-args.jsonl")
    async with aiofiles.open(args_file_path, "w") as f:
        for arg in thread_scraper_args:
            await f.write(json.dumps(arg) + "\n")
    job_id = str(FlowRunContext.get().flow_run.id) + "-thread"
    thread_scraper_job = await scrapy_tasks.run_scraper("openjdk-mailman2-thread", job_id, args={"arguments_file": args_file_path})

    return await util.read_jsonl_from_file(thread_scraper_job["items_url"])


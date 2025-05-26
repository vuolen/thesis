import os
import asyncio
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
from prefect_project import scrapyd_client, env

@task(cache_policy=NO_CACHE)
async def run_scraper(spider_name: str, job_id: str, args={}):
    logger = get_run_logger()

    # with open("/home/lennu/code/thesis/data/feeds/scrapy_project/openjdk-mailman2-mailing-lists/bd1083e7-7520-42e2-bb8b-4c2d1a40344e.jl", "r") as items:
    #     items = [json.loads(line) for line in items.readlines()]
    #     for item in items:
    #         logger.info(f"Yielding item {item['name']}")
    #         yield item

    # return

    if await scrapyd_client.is_spider_running(spider_name):
        raise Exception(f"Spider {spider_name} is running or pending. Aborting deployment")

    created_job = await scrapyd_client.schedule_spider(spider_name, job_id, settings = {
        "FILES_STORE": env.FILES_DIR,
        "HTTPCACHE_ENABLED": True
    }, args=args)
    logger.info(f"Scheduled job {created_job['jobid']} for spider {spider_name}")

    full_job_status = None

    while True:
        jobs = await scrapyd_client.listjobs()
        job = jobs.get(created_job["jobid"])
        if job is not None:
            logger.debug(f"Fetched job status {job}")
            full_job_status = job
            if job["status"] == "finished":
                break
        await asyncio.sleep(60)

    if "items_url" in full_job_status:
        full_job_status["items_url"] = os.path.join(os.getcwd(), full_job_status["items_url"].replace("/items", env.ITEM_FEEDS_DIR))
    if "log_url" in full_job_status:
        full_job_status["log_url"] = os.path.join(os.getcwd(), full_job_status["log_url"].lstrip("/"))

    return full_job_status

async def cleanup_scrapy(flow, flow_run, state):
    await scrapyd_client.kill_job(flow_run.id, force=True)
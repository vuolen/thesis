import os
import asyncio
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry

SCRAPYD_URL = os.getenv("SCRAPYD_URL")

RETRY_OPTIONS = ExponentialRetry(attempts=100)

def get_client():
    return RetryClient(
        retry_options=RETRY_OPTIONS,
        timeout=ClientTimeout(total=20),
    )

async def listjobs():
    async with get_client() as session:
        async with session.get(
                f"{SCRAPYD_URL}/listjobs.json",
                params={"project": "scrapy_project"},
                raise_for_status=True
            ) as resp:
                jobs = await resp.json()
                return jobs
        

async def get_running_and_pending_jobs():
    jobs = await listjobs()
    return jobs.get("running", []) + jobs.get("pending", [])

async def is_spider_running(spider_name):
    print(f"Checking if spider {spider_name} is running")
    jobs = await get_running_and_pending_jobs()
    for job in jobs:
        if job["spider"] == spider_name:
            return True
    return False


async def schedule_spider(spider_name, job_id, settings):
    print(f"Deploying spider {spider_name}, with settings {settings}")


    data = [
        ("project", "scrapy_project"),
        ("spider", spider_name),
        ("jobid", job_id),
        *[
            ("setting", f"{key}={value}")
            for key, value in settings.items()
        ]
    ]

    async with get_client() as session:
        async with session.post(
            f"{SCRAPYD_URL}/schedule.json",
            data=data,
            raise_for_status=True
        ) as response:
            job = await response.json()
            print(f"Deployed spider {spider_name}.")
            return job

async def kill_job(job_id, force=True):
    print(f"Canceling job {job_id}.")
    async with get_client() as session:
        async with session.post(
            f"{SCRAPYD_URL}/cancel.json",
            data={
                "project": "scrapy_project",
                "job": job_id,
                "signal": "KILL" if force else "TERM",
            },
            raise_for_status=True
        ) as resp:
            return await resp.json()
        
async def cleanup(force=True):
    print(f"Starting cleanup")
    while True:
        running_and_pending = await get_running_and_pending_jobs()

        if len(running_and_pending) == 0:
            break

        for job in running_and_pending:
            await kill_job(job["id"], force=force)

        await asyncio.sleep(5)

    print("Cleanup done")
            
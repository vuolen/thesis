import asyncio
import os
import sys

SEARCH_DIR = "searches"
SCRAPE_DIR = "thesis_scraper/scraped"
PATTERN_DIR = "patterns"

async def run_command(command):
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    return (stdout, stderr)

async def pdfgrep_search(scrape_dir, pattern_file):
    print(f"pdfgrepping {scrape_dir}, {pattern_file}")
    stdout, stderr = await run_command(f"pdfgrep -rioHP -m 1 -f {os.path.join(PATTERN_DIR, pattern_file)} {os.path.join(SCRAPE_DIR, scrape_dir)}")
    return stdout, stderr

async def zgrep_search(scrape_dir, pattern_file):
    print(f"zgrepping {scrape_dir}, {pattern_file}")
    stdout, stderr = await run_command(f"zgrep -rioHEa -m 1 -f {os.path.join(PATTERN_DIR, pattern_file)} {os.path.join(SCRAPE_DIR, scrape_dir)}")
    return stdout, stderr

async def search(collection, pattern):
    zstdout, zstderr = await zgrep_search(collection, pattern + ".txt")
    pdfstdout, pdfstderr = await pdfgrep_search(collection, pattern + ".txt")

    print(collection, pattern)
    if zstderr == b"" and pdfstderr == b"":
        print("\tOkay!")
        output_dir = os.path.join(SEARCH_DIR, collection, pattern)
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "zgrep.txt"), "wb") as f:
            f.write(zstdout)
        with open(os.path.join(output_dir, "pdfgrep.txt"), "wb") as f:
            f.write(pdfstdout)
    else:
        print("\tError!")
        print(zstderr)
        print(pdfstderr)


async def main():
    patterns = [file.split(".")[0] for file in os.listdir(PATTERN_DIR)]
    collections = os.listdir(SCRAPE_DIR)
    searches = [search(collection, pattern) for pattern in patterns for collection in collections]
    #pdfgreps = [pdfgrep_search(collection, pattern) for pattern in patterns for collection in collections]
    await asyncio.gather(*searches)



if __name__ == "__main__":
    asyncio.run(main())
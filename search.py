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
    stdout, stderr = await run_command(f"zgrep -rioHEaF -m 1 -f {os.path.join(PATTERN_DIR, pattern_file)} {os.path.join(SCRAPE_DIR, scrape_dir)}")
    return stdout, stderr

async def ripgrep_search(scrape_dir, pattern_file):
    print(f"ripgrepping {scrape_dir}, {pattern_file}")
    stdout, stderr = await run_command(f"rg -iHF --count-matches -f {os.path.join(PATTERN_DIR, pattern_file)} {os.path.join(SCRAPE_DIR, scrape_dir)}")
    lines = filter(lambda line: line != b"", stdout.split(b"\n"))
    lines = sorted(lines, key=lambda line: int(line.split(b":")[1]), reverse=True)
    stdout = b"\n".join(lines)
    return stdout, stderr, "ripgrep"

SEARCHES = {
    "python-mailman2-mailing-lists": [ripgrep_search],
    "python-mailman3-mailing-lists": [ripgrep_search],
    "openjdk-mailman2-mailing-lists": [ripgrep_search]
}

async def search(collection, pattern):

    searches = SEARCHES.get(collection, [])

    for search in searches:
        stdout, stderr, commandName = await search(collection, pattern + ".txt")

        if stderr == b"":
            output_dir = os.path.join(SEARCH_DIR, collection, pattern)
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, f"{commandName}.txt"), "wb") as f:
                f.write(stdout)
        else:
            print(stderr)


async def main(patterns=None, collections=None):
    all_patterns = [file.split(".")[0] for file in os.listdir(PATTERN_DIR)]
    all_collections = os.listdir(SCRAPE_DIR)

    patterns = [pattern for pattern in patterns if pattern in all_patterns] if patterns else all_patterns
    collections = [collection for collection in collections if collection in all_collections] if collections else all_collections

    print("Searching for patterns", patterns, "in collections", collections)
    searches = [search(collection, pattern) for pattern in patterns for collection in collections]
    #pdfgreps = [pdfgrep_search(collection, pattern) for pattern in patterns for collection in collections]
    await asyncio.gather(*searches)


import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for patterns in collections")
    parser.add_argument("--patterns", type=str, help="The patterns to search for")
    parser.add_argument("--collections", type=str, help="The collections to search in")
    args = parser.parse_args()
    patterns = args.patterns.split(",") if args.patterns else None
    collections = args.collections.split(",") if args.collections else None
    asyncio.run(main(patterns, collections))
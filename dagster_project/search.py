import asyncio
import os

SEARCH_DIR = "searches"
SCRAPE_DIR = "final_data/thesis_scraper/scraped"







async def ripgrepAll(file):
    pass






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

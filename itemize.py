from search import SEARCH_DIR
import argparse
import os
import glob

ITEMS_DIR = os.path.join("./items")


for collectionDir in os.listdir(SEARCH_DIR):
    for patternFile in os.listdir(os.path.join(SEARCH_DIR, collectionDir)):
        for file in os.listdir(os.path.join(SEARCH_DIR, collectionDir, patternFile)):
            print(os.path.join(SEARCH_DIR, collectionDir, patternFile, file))
            matches = []
            with open(os.path.join(SEARCH_DIR, collectionDir, patternFile, file), "r") as f:
                for line in f.readlines():
                    fileName = line.split(":")[0]
                    n_matches = line.split(":")[1]
                    itemName = os.path.dirname(fileName)

                    matches.append((itemName, n_matches))
            
            items = {}
            for item, n_matches in matches:
                if item not in items:
                    items[item] = 0
                items[item] += int(n_matches) 

            os.makedirs(os.path.join(ITEMS_DIR, collectionDir, patternFile), exist_ok=True)

            sorted_items = sorted(items.items(), key=lambda x: x[1], reverse=True)

            with open(os.path.join(ITEMS_DIR, collectionDir, patternFile, "items.txt"), "w") as f:
                for item, n_matches in sorted_items:
                    itemName = item.split("/")[-1]
                    f.write(f"{itemName}: {n_matches}\n")
import os

PATTERN_DIR = "patterns"
PATTERN_FILES = os.listdir(PATTERN_DIR)

PATTERNS = {}

for file in PATTERN_FILES:
    with open(os.path.join(PATTERN_DIR, file), "r") as f:
        file_patterns = f.readlines()
        file_patterns = [x.strip() for x in file_patterns]
        feature = file.split(".")[0]
        PATTERNS[feature] = file_patterns
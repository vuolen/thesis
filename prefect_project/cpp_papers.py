import os
from chardet.universaldetector import UniversalDetector
from prefect import get_run_logger

FILES_DIR = os.getenv("FILES_DIR")

def fix_encoding(file, item):
    logger = get_run_logger()
    try:
        detector = UniversalDetector()
        if file["path"].endswith(".html") or file["path"].endswith(".htm"):
            detector.reset()
            content = b""
            charset = None
            with open(os.path.join(FILES_DIR, file["path"]), "rb") as f:
                for line in f.readlines():
                    if b"charset=" in line:
                        charset = line.split(b"charset=")[1].split(b'"')[0]
                    content += line
                    if not detector.done and charset is None:
                        detector.feed(line)
            detector.close()

            encoding = charset if charset else detector.result["encoding"]
            decoded = content.decode(encoding, errors="strict")

            return {
                "stdin": decoded,
            }
    except Exception as e:
        logger.error(f"Error encoding file {file['path']} from item {item}: {e}")

    return file

def to_documents(item):
    return [{
        **item,
        "files": [fix_encoding(file, item) for file in item["files"]]
    }]
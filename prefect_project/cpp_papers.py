import os
import re
import chardet
from prefect import get_run_logger


FILES_DIR = os.getenv("FILES_DIR")

CHARSET_REGEX = br"charset=([a-zA-Z0-9\-_]+)"

def fix_encoding(file, item):
    logger = get_run_logger()
    try:
        if file["path"].endswith(".html") or file["path"].endswith(".htm"):
            content = b""
            charset = None
            with open(os.path.join(FILES_DIR, file["path"]), "rb") as f:
                for line in f.readlines():
                    match = re.search(CHARSET_REGEX, line)
                    if match:
                        charset = match.group(1).decode("utf-8")
                content += line

            potential_encodings = [
                chardet.detect(content)["encoding"],
                charset,
                "utf-8",
                "iso-8859-1",
                "windows-1252",
                "windows-1251"
            ]

            decoded = None

            for encoding in potential_encodings:
                try:
                    decoded = content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue

            if decoded is None:
                logger.error(f"Failed to decode file {file['path']} from item {item}")
                return file

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
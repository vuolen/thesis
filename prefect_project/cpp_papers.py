import os

FILES_DIR = os.getenv("FILES_DIR")

def fix_encoding(file):
    if file["path"].endswith(".html") or file["path"].endswith(".htm"):
        with open(os.path.join(FILES_DIR, file["path"]), "rb") as f:
            head = f.read(1000)
            if "charset=windows-1252" in head.lower():
                content = head + f.read()
                return {
                    "stdin": content.decode("windows-1252")
                }

    return file

def to_documents(item):
    return [{
        **item,
        "files": [fix_encoding(file) for file in item["files"]]
    }]


def to_document(item):
    def fix_encoding(file):
        if file["path"].endswith(".html") or file["path"].endswith(".htm"):
            with open(file["path"], "rb") as f:
                head = f.read(1000)
                if "charset=windows-1252" in head.lower():
                    content = head + f.read()
                    return {
                        "stdin": content.decode("windows-1252")
                    }

        return file

    return {
        **item,
        "files": [fix_encoding(file) for file in item["files"]]
    }
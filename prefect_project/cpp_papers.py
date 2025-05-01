import os
from chardet.universaldetector import UniversalDetector

FILES_DIR = os.getenv("FILES_DIR")

def fix_encoding(file):
    detector = UniversalDetector()
    if file["path"].endswith(".html") or file["path"].endswith(".htm"):
        detector.reset()
        content = b""
        with open(os.path.join(FILES_DIR, file["path"]), "rb") as f:
            for line in f.readlines():
                content += line
                detector.feed(line)
                if detector.done:
                    break
        detector.close()

        if detector.confidence < 0.9:
            print(f"Warning: low confidence in encoding detection for {file['path']}: {detector.confidence}")

        encoding = detector.result["encoding"]

        decoded = content.decode(encoding, errors="strict")

        return {
            "stdin": decoded,
        }



    return file

def to_documents(item):
    return [{
        **item,
        "files": [fix_encoding(file) for file in item["files"]]
    }]
import os
import json
from nicegui import ui, app
from dotenv import load_dotenv

load_dotenv()

ITEM_FEEDS_DIR = os.getenv("ITEM_FEEDS_DIR")
FILES_DIR = os.getenv("FILES_DIR")


@ui.page('/')
def main():
    ITEM_FEEDS = [
        file
        for file in os.listdir(ITEM_FEEDS_DIR)
        if file.endswith("final.jsonl")
    ]
    ui.label("Collections")
    for feed in ITEM_FEEDS:
        collection_name = feed.split("-final.jsonl")[0]
        ui.link(
            collection_name,
            f"/{collection_name}",
        )

@ui.page('/{collection}')
def collection(collection: str):
    with open(os.path.join(ITEM_FEEDS_DIR, f"{collection}-final.jsonl")) as file:
        data = file.readlines()
        items = [
            json.loads(line)
            for line in data
        ]

        if len(items) == 0:
            ui.label("No items found")
            return

        columns = [
            { "name": "name", "label": "Name", "field": "name", "align": "left", "sortable": False, },
            *[
                { "name": key, "label": key, "field": key, "align": "left", "sortable": True, }
                for key in items[0]["matches"].keys()
            ]
        ]

        rows = [
            {
                "collection": collection,
                "id": item["id"],
                "name": item["name"],
                **{
                    k: v
                    for k, v in item["matches"].items()
                },
            }
            for item, i in zip(items, range(len(items)))
        ]

        table = ui.table(
            rows=rows,
            columns=columns,
            pagination={
                "rowsPerPage": 200,
            },
        ).style('width: calc(100vw - 2rem); height: calc(100vh - 2rem);')
        table.add_slot('body-cell-name', 
            '''
            <q-td :props="props">
                <a :href="props.row.collection + '/' + props.row.id">{{ props.value }}</a>
            </q-td>
            ''')

def get_item_by_id(collection: str, id: str):
    with open(os.path.join(ITEM_FEEDS_DIR, f"{collection}-final.jsonl")) as file:
        data = file.readlines()
        for line in data:
            item = json.loads(line)
            if item["id"] == id:
                return item
    return None

def serve_item_url(collection: str, id: str, urlIndex: int):
    item = get_item_by_id(collection, id)
    if item:
        ui.html(f'<iframe src="{item["file_urls"][urlIndex]}" style="width: calc(100vw - 2rem); height: calc(100vh - 2rem);" />')
    else:
        ui.label("Item not found")

def navigate_to_item_url(collection: str, id: str, urlIndex: int):
    item = get_item_by_id(collection, id)
    if item:
        ui.navigate.to(item["file_urls"][urlIndex])
    else:
        ui.label("Item not found")

@ui.page('/java-jep/{id}')
def java_jep(id: str):
    item = get_item_by_id("java-jep", id)
    if item:
        with open(os.path.join(FILES_DIR, item["files"][0]["path"]), "r") as file:
            content = file.read()
            ui.add_body_html(content)
    else:
        ui.label("Item not found")

@ui.page('/java-specs/{id}')
def java_specs(id: str):
    item = get_item_by_id("java-specs", id)
    if item:
        ui.html(f'<embed src="/files/{item["files"][0]["path"]}" type="application/pdf" style="width: calc(100vw - 2rem); height: calc(100vh - 2rem);" />')
    else:
        ui.label("Item not found")

@ui.page('/python-pep/{id}')
def python_pep(id: str):
    serve_item_url("python-pep", id, 0)

@ui.page('/cpp-mailing-lists/{id}')
def cpp_mailing_lists(id: str):
    navigate_to_item_url("cpp-mailing-lists", id, -1)

app.add_static_files('/files', FILES_DIR)

ui.run()
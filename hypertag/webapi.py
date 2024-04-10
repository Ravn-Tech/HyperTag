import os
import re
import sys
from pathlib import Path
import json
import threading
import torch
import rpyc  # type: ignore
from rpyc.utils.server import ThreadedServer  # type: ignore
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import uvicorn
import subprocess
from pathlib import Path
from watchdog.observers import Observer  # type: ignore
from watchdog.events import FileSystemEventHandler  # type: ignore
from .hypertag import HyperTag
from .persistor import Persistor
from .vectorizer import TextVectorizer, CLIPVectorizer
from .utils import update_symlink
from .daemon import auto_importer, watch_hypertagfs


app = FastAPI()
ht = HyperTag()
text_vectorizer = None
image_vectorizer = None

app.mount("/site", StaticFiles(directory="./hypertag/client", html = True), name="site")


@app.get("/get_file_name/{fileid}")
async def get_file_name(fileid: int):
    name = ht.db.get_file_name_by_id(fileid)
    print("FILENAME", name)
    return {"result": name}

@app.get("/files")
async def files():
    return {"files": [[x,y] for y,x in ht.db.get_files(False, True)]}

@app.get("/tags")
async def tags():
    return {"tags": ht.show(mode="tags", path=False, print_=False)}

@app.get("/get_tags/{file_id}")
async def get_tags(file_id: int):
    return {"tags": ht.db.get_tags_by_file_id(file_id)}

@app.get("/add_tags/{file_id}/{tag_string}")
async def add_tags(file_id: int, tag_string: str):
    print("Adding", tag_string, "to", file_id)
    for tag in tag_string.split(","):
        clean_tag = tag.strip()
        ht.db.add_tag_to_file_id(clean_tag, file_id)
    return {"tags": ht.db.get_tags_by_file_id(file_id)}

@app.get("/find/{query}")
async def open(query: str):
    query = str(query.replace("$", "/").strip())
    print("FIND:", query)
    
    if query.startswith('"') and query.endswith('"'):
        # search for file_names containing the query
        # TODO: Add exact string matching for file text content
        query = query[1:len(query)-1] # removes ""
        results = ht.db.get_files_by_name(query)#.get_files(show_path=True, include_id=True)
    elif query.startswith('='):
        # Tag Search
        query = query.replace("=", "")
        query_list = query.split(" ")
        print("QLIST", query_list)
        results = list([ht.db.get_file_id_by_name(fname), fname] for fname in ht.query(query_list[0], *tuple(query_list[1:])))
    else:
        results = []

    print(results)
    return {"results": results}

@app.get("/open/{file_id}")
async def open(file_id: int):
    filepath = Path(ht.db.get_file_path_by_id(file_id)) # convert to path and strip whitespace

    # Open the file
    if sys.platform.startswith('darwin'): # macosx
        subprocess.call(('open', filepath))
    elif os.name == 'nt': # windows
        os.startfile(filepath)
    elif os.name == 'posix': # linux
        subprocess.call(('xdg-open', filepath))
    print("Opening", filepath, ".:.")

    return {"status": "OK", "path": str(filepath)}

def get_service_name(): return "HyperTag - WebAPI Service"

def compute_text_embedding(self, sentences_json):
    if text_vectorizer is not None:
        sentences = json.loads(sentences_json)
        return json.dumps(text_vectorizer.compute_text_embedding(sentences))

def start(cpu, text, image):
    # Spawn Auto-Importer threads
    auto_importer()
    # Spawn HyperTagFS watch in thread
    watch_hypertagfs()

    cuda = torch.cuda.is_available()
    if cuda:
        print("CUDA runtime available")
    else:
        print("CUDA runtime not available (this might take a while)")
    if (text and image is None) or (image and text) or (not image and not text):
        print("Initializing TextVectorizer...")
        global text_vectorizer
        text_vectorizer = TextVectorizer(verbose=True)
    if (image and text is None) or (image and text) or (not image and not text):
        print("Initializing ImageVectorizer...")
        global image_vectorizer
        image_vectorizer = CLIPVectorizer(cpu, verbose=True)

    # HTTP
    port = 23232
    print("Starting UVICORN HTTP Server .:. on Port:", port, "\nDomain-Dir: http://localhost:23232/site/")
    uvicorn.run(app, host="0.0.0.0", port=port)



if __name__ == "__main__":
    print("Starting HyperTag WebAPI server as standalone process")
    start(1, None, None)

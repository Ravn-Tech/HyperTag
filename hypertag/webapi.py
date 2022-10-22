import os
import re
from pathlib import Path
import json
import threading
import torch
import rpyc  # type: ignore
from rpyc.utils.server import ThreadedServer  # type: ignore
from fastapi import FastAPI
import uvicorn
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


@app.get("/tags")
async def tags():
    return {"tags": ht.show(mode="tags", path=False, print=False)}

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
    print("Starting UVICORN HTTP Server.:.")
    uvicorn.run(app, host="0.0.0.0", port=port)



if __name__ == "__main__":
    print("Starting HyperTag WebAPI server as standalone process")
    start(1, None, None)

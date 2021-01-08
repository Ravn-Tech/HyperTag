""" Daemon process monitoring changes in HyperTagFS """
import os
import re
from pathlib import Path
import json
import threading
import torch
import rpyc  # type: ignore
from rpyc.utils.server import ThreadedServer  # type: ignore
from watchdog.observers import Observer  # type: ignore
from watchdog.events import FileSystemEventHandler  # type: ignore
from .persistor import Persistor
from .vectorizer import TextVectorizer, CLIPVectorizer

text_vectorizer = None
image_vectorizer = None


class DaemonService(rpyc.Service):
    def on_connect(self, conn):
        print("New connection:", conn)

    def on_disconnect(self, conn):
        pass

    def exposed_compute_text_embedding(self, sentences_json):
        if text_vectorizer is not None:
            sentences = json.loads(sentences_json)
            return json.dumps(text_vectorizer.compute_text_embedding(sentences))

    def exposed_search(self, text_query: str, path=False, top_k=10, score=False):
        if text_vectorizer is not None:
            return text_vectorizer.search(text_query, path, top_k, score)

    def exposed_encode_image(self, path: str):
        if image_vectorizer is not None:
            return json.dumps(image_vectorizer.encode_image(path).tolist())

    def exposed_search_image(self, text_query: str, path=False, top_k=10, score=False):
        if image_vectorizer is not None:
            return image_vectorizer.search_image(text_query, path, top_k, score)


class ChangeHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()

    def on_moved(self, event):
        # Change tag association of moved file / dir
        super().on_moved(event)

        what = "directory" if event.is_directory else "file"
        print("Moved", what, ": from", event.src_path, "to", event.dest_path)

    def on_created(self, event):
        # DONE Interpret as query and populate with results as symlinks
        # TODO: Save created query dirs in dedicated table to recreate later
        super().on_created(event)

        what = "directory" if event.is_directory else "file"
        print("Created", what, event.src_path)
        if event.is_directory:
            from .hypertag import HyperTag

            ht = HyperTag()
            print("Populating with query results...")
            path = Path(event.src_path)
            query = path.name
            args = re.findall(r"'.*?'|\".*?\"|\S+", query)
            args = [e.replace('"', "").replace("'", "") for e in args]
            top_k = [int(a.split("=")[-1]) for a in args if a.startswith("top_k=")]
            if top_k:
                top_k = top_k[0]
            else:
                top_k = 10
            args = [a for a in args if not a.startswith("top_k=")]

            parent_tag_name = path.parent.name
            if parent_tag_name == "Search Texts":
                query_results = ht.search(*args, path=True, top_k=top_k, _return=True)
            elif parent_tag_name == "Search Images":
                query_results = ht.search_image(*args, path=True, top_k=top_k, _return=True)
            else:
                query_results = ht.query(*args, path=True)
            try:
                for fp in query_results:
                    file_path = Path(fp)
                    os.symlink(file_path, path / file_path.name)
            except Exception as ex:
                print("Failed:", ex)

    def on_deleted(self, event):
        # DONE For symlink remove tag
        # DONE: For directory remove all tag associations
        super().on_deleted(event)

        what = "directory" if event.is_directory else "file"
        path = Path(event.src_path)
        print("Deleted", what, path)
        if event.is_directory:
            tag_name = path.name
            parent_tag_name = path.parent.name
            if parent_tag_name in {"HyperTagFS", "Search Texts", "Search Images"}:
                return
            print(
                "Removing parent tag association for:",
                tag_name,
                "Parent:",
                parent_tag_name,
            )
            with Persistor() as db:
                db.remove_parent_tag_from_tag(parent_tag_name, tag_name)
        else:  # Symlink
            tag_name = path.parent.name
            if tag_name == "_files":
                tag_name = path.parent.parent.name
            elif tag_name == "HyperTagFS":
                return
            file_name = path.name
            print("Removing tag:", tag_name, "from file:", file_name)
            with Persistor() as db:
                db.remove_tag_from_file(tag_name, file_name)

    def on_modified(self, event):
        super().on_modified(event)

        what = "directory" if event.is_directory else "file"
        print("Modified", what, ":", event.src_path)


def watch():
    with Persistor() as db:
        path = db.get_hypertagfs_dir()
    print("Watching HyperTagFS:", path)
    event_handler = ChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()


def start():
    # Spawn HyperTagFS watch in thread
    t = threading.Thread(target=watch)
    t.start()

    cuda = torch.cuda.is_available()
    if cuda:
        print("Using CUDA runtime")
    else:
        print("CUDA runtime not available (this might take a while)")
    print("Initializing TextVectorizer...")
    global text_vectorizer
    text_vectorizer = TextVectorizer()
    print("Initializing ImageVectorizer...")
    global image_vectorizer
    image_vectorizer = CLIPVectorizer()

    # IPC
    port = 18861
    # TODO: Investigate why Vectorize seems to be initialized twice...
    t = ThreadedServer(DaemonService, port=port)
    print(f"Starting DaemonService, listening on: localhost:{port}")
    t.start()


if __name__ == "__main__":
    print("Starting as standalone process")
    start()

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
from .vectorizer import Vectorizer

cuda = torch.cuda.is_available()
if cuda:
    print("Using CUDA to speed stuff up")
else:
    print("CUDA not available (this might take a while)")
vectorizer = Vectorizer()


class DaemonService(rpyc.Service):
    def on_connect(self, conn):
        print("New connection:", conn)

    def on_disconnect(self, conn):
        pass

    def exposed_compute_text_embedding(self, sentences_json):
        sentences = json.loads(sentences_json)
        return vectorizer.compute_text_embedding(sentences)

    def exposed_search(self, text_query: str, path=False, top_k=10, score=False):
        return vectorizer.search(text_query, path, top_k, score)


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
        # TODO: Save created query dirs in dedicated to table to recreate later
        super().on_created(event)

        what = "directory" if event.is_directory else "file"
        print("Created", what, event.src_path)
        if event.is_directory:
            print("Populating with query results...")
            path = Path(event.src_path)
            query = path.name
            from .hypertag import HyperTag

            ht = HyperTag()
            try:
                args = re.findall(r"'.*?'|\".*?\"|\S+", query)
                args = [e.replace('"', "") for e in args]
                args = [e.replace("'", "") for e in args]
                query_results = ht.query(*args, path=True)
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
            if parent_tag_name == "HyperTagFS":
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
    # IPC
    port = 18861
    # TODO: Investigate why Vectorize seems to be initialized twice...
    t = ThreadedServer(DaemonService, port=port)
    print(f"Starting DaemonService, listening on: localhost:{port}")
    t.start()


if __name__ == "__main__":
    print("Starting as standalone process")
    start()

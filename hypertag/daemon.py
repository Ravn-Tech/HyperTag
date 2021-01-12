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
from .utils import update_symlink

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

    def exposed_update_text_index(self):
        if text_vectorizer is not None:
            corpus_vectors, corpus_paths = text_vectorizer.get_text_corpus()
            text_vectorizer.update_index(len(corpus_vectors))

    def exposed_encode_image(self, path: str):
        if image_vectorizer is not None:
            return json.dumps(image_vectorizer.encode_image(path).tolist())

    def exposed_search_image(self, text_query: str, path=False, top_k=10, score=False):
        if image_vectorizer is not None:
            return image_vectorizer.search_image(text_query, path, top_k, score)

    def exposed_update_image_index(self):
        if image_vectorizer is not None:
            corpus_vectors, corpus_paths = image_vectorizer.get_image_corpus()
            image_vectorizer.update_index(len(corpus_vectors))


class AutoImportHandler(FileSystemEventHandler):
    def __init__(self, import_path):
        super().__init__()
        self.import_path = import_path
        with Persistor() as db:
            self.auto_index_images = db.get_auto_index_images(import_path)
            self.auto_index_texts = db.get_auto_index_texts(import_path)
        if self.auto_index_images:
            print("AutoImportHandler - Auto indexing images for", import_path)
        if self.auto_index_texts:
            print("AutoImportHandler - Auto indexing texts for", import_path)

    def on_moved(self, event):
        # TODO: Handle file rename / move
        super().on_moved(event)
        what = "directory" if event.is_directory else "file"
        print("AutoImportHandler - Moved", what, ": from", event.src_path, "to", event.dest_path)
        path = Path(event.dest_path)
        with Persistor() as db:
            file_id = db.get_file_id_by_path(event.src_path)

        from .hypertag import HyperTag

        ht = HyperTag()
        if file_id is None and path.is_file:  # Add new file
            print("AutoImportHandler - Adding file:", path)
            ht.add(str(path))
            import_path_dirs = set(str(self.import_path).split("/"))
            print("AutoImportHandler - Adding tags...")
            ht.auto_add_tags_from_path(path, import_path_dirs)
            ht.db.conn.commit()
            ht.mount(ht.root_dir)
            if self.auto_index_images:
                ht.index_images()
            if self.auto_index_texts:
                ht.index_texts()
        elif file_id is not None:  # Update existing path
            print("AutoImportHandler - Updating path & name for:", event.src_path, "to", path)
            old_name = event.src_path.split("/")[-1]
            with Persistor() as db:
                db.update_file_by_id(file_id, path)
                hypertagfs_path = db.get_hypertagfs_dir()
            # Update broken symlinks
            update_symlink(hypertagfs_path, old_name, path)
            # Add possible new tags
            import_path_dirs = set(str(event.src_path).split("/"))
            ht.auto_add_tags_from_path(path, import_path_dirs, verbose=True, keep_all=True)
            ht.db.conn.commit()
            # Remount
            ht.mount(ht.root_dir)

    def on_created(self, event):
        # Start auto import
        super().on_created(event)

        what = "directory" if event.is_directory else "file"
        print("AutoImportHandler - Created", what, event.src_path)
        path = Path(event.src_path)
        is_download_file = str(path).endswith(".crdownload")
        if path.is_file() and not is_download_file:  # Ignore download progress file
            from .hypertag import HyperTag

            ht = HyperTag()
            print("AutoImportHandler - Adding file:", path)
            ht.add(str(path))
            import_path_dirs = set(str(self.import_path).split("/"))
            print("AutoImportHandler - Adding tags...")
            ht.auto_add_tags_from_path(path, import_path_dirs)
            ht.db.conn.commit()
            ht.mount(ht.root_dir)
            if self.auto_index_images:
                ht.index_images()
            if self.auto_index_texts:
                ht.index_texts()

    def on_deleted(self, event):
        # TODO: Remove file and its tags
        super().on_deleted(event)

        what = "directory" if event.is_directory else "file"
        path = Path(event.src_path)
        print("AutoImportHandler - Deleted", what, path)


class HyperTagFSHandler(FileSystemEventHandler):
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
        path = Path(event.src_path)
        query = path.name
        if event.is_directory and not query.startswith("_"):
            from .hypertag import HyperTag

            ht = HyperTag()
            print("Populating with query results...")
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
        """For symlink: remove tag
        For directory: remove tag associations"""
        super().on_deleted(event)

        what = "directory" if event.is_directory else "file"
        path = Path(event.src_path)
        print("Deleted", what, path)
        if event.is_directory and not path.name.startswith("_"):
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


def spawn_observer_thread(event_handler, path):
    print("Spawned observer for:", path)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    t = threading.Thread(target=observer.start)
    t.start()


def auto_importer():
    with Persistor() as db:
        auto_import_paths = db.get_auto_import_paths()
    print("Spawning Auto-Importer Threads...")
    for import_path in auto_import_paths:
        event_handler = AutoImportHandler(import_path)
        spawn_observer_thread(event_handler, import_path)


def watch_hypertagfs():
    with Persistor() as db:
        path = db.get_hypertagfs_dir()
    print("Spawning HyperTagFS Watching Thread...")
    event_handler = HyperTagFSHandler()
    spawn_observer_thread(event_handler, path)


def start():
    # Spawn Auto-Importer threads
    auto_importer()
    # Spawn HyperTagFS watch in thread
    watch_hypertagfs()

    cuda = torch.cuda.is_available()
    if cuda:
        print("Using CUDA runtime")
    else:
        print("CUDA runtime not available (this might take a while)")
        # TODO: Only TextVectorizer works without CUDA right now
    print("Initializing TextVectorizer...")
    global text_vectorizer
    text_vectorizer = TextVectorizer(verbose=True)
    print("Initializing ImageVectorizer...")
    global image_vectorizer
    image_vectorizer = CLIPVectorizer(verbose=True)

    # IPC
    port = 18861
    # TODO: Investigate why Vectorize seems to be initialized twice...
    t = ThreadedServer(DaemonService, port=port)
    print(f"Starting DaemonService, listening on: localhost:{port}")
    t.start()


if __name__ == "__main__":
    print("Starting as standalone process")
    start()

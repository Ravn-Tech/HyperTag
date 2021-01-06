import os
import multiprocessing
from multiprocessing import Process
from shutil import rmtree
import sqlite3
from pathlib import Path
import json
from typing import List, Tuple
import fire  # type: ignore
from tqdm import tqdm  # type: ignore
import filetype  # type: ignore
from .persistor import Persistor
from .daemon import start
from .graph import graph


class HyperTag:
    """ HyperTag CLI """

    def __init__(self):
        self.db = Persistor()
        self.root_dir = Path(self.db.get_hypertagfs_dir())
        os.makedirs(self.root_dir, exist_ok=True)

    def get_text_documents(self, file_paths: List[str]) -> List[Tuple[str, str]]:
        doc_id = self.db.get_tag_id_by_name("Documents")
        doc_types = set()
        for tag_id, _type_name in self.db.get_tag_id_children_ids_names(doc_id):
            doc_types.add(self.db.get_tag_name_by_id(tag_id))

        # Keep only text files
        compatible_files = []
        for file_path in file_paths:
            file_type_guess = filetype.guess(str(file_path))
            if file_type_guess is None:
                continue
            file_type = file_type_guess.extension.lower()
            if file_type in doc_types:
                compatible_files.append((str(file_path), file_type))
        return compatible_files

    def index(self, rebuild=False, cache=False):
        """ Vectorize text files (needed for semantic search) """
        # TODO: index images
        # TODO: auto index on file addition (import)
        print("Vectorizing text documents...")
        from .vectorizer import (
            extract_clean_text,
            compute_text_embedding,
        )
        import torch

        cuda = torch.cuda.is_available()
        if cuda:
            print("Using CUDA to speed stuff up")
        if rebuild:
            file_paths = self.db.get_indexed_file_paths()
        else:
            file_paths = self.db.get_unindexed_file_paths()
        i = 0
        compatible_files = self.get_text_documents(file_paths)
        min_words = 5
        min_word_length = 4
        args = []
        for file_path, file_type in compatible_files:
            args.append((file_path, file_type, cache, min_words, min_word_length))
        inference_tuples = []

        # Preprocess using multi-processing
        pool = multiprocessing.Pool(processes=8)
        print("Preprocessing...")
        with tqdm(total=len(compatible_files)) as t:
            for file_path, sentences in pool.imap_unordered(extract_clean_text, args):
                t.update(1)
                if sentences:
                    inference_tuples.append((file_path, sentences))
        print(f"Cleaned {len(inference_tuples)} text docs successfully!")
        print("Starting inference...")
        # Compute embeddings
        for file_path, sentences in tqdm(inference_tuples):
            document_vector = compute_text_embedding(sentences)
            if (
                document_vector is not None
                and type(document_vector) is list
                and len(document_vector) > 0
            ):
                self.db.add_file_embedding_vector(file_path, json.dumps(document_vector))
                self.db.conn.commit()
                i += 1
            else:
                print(type(document_vector))
                print("Failed to parse file - skipping:", file_path)
        print(f"Vectorized {str(i)} file/s.")

    def search(self, text_query: str, path=False, top_k=10, score=False):
        """ Execute a semantic search that returns best matching text documents """
        from .vectorizer import (
            clean_transform,
            compute_text_embedding,
            vector_search,
        )

        text_document_tuples = self.get_text_documents(self.db.get_files(show_path=True))
        text_document_paths = [path for path, _file_type in text_document_tuples]
        corpus = self.db.get_file_embedding_vectors(text_document_paths)
        if len(corpus) == 0:
            print("No relevant files indexed...")
            return
        sentence_query = clean_transform(text_query, 0, 1)
        query_vector = compute_text_embedding(sentence_query)
        corpus_paths = []
        corpus_vectors = []
        for doc_path, embedding_vector in corpus:
            corpus_vectors.append(json.loads(embedding_vector))
            corpus_paths.append(doc_path)

        top_matches = vector_search(query_vector, corpus_vectors, top_k=top_k)
        for match in top_matches[0]:
            corpus_id, score_value = match["corpus_id"], match["score"]
            file_path = corpus_paths[corpus_id]
            file_name = file_path.split("/")[-1]
            if path:
                file_name = file_path
            if score:
                print(file_name, f"({score_value})")
            else:
                print(file_name)

    def set_hypertagfs_dir(self, path: str):
        """ Set path for HyperTagFS directory """
        self.db.set_hypertagfs_dir(path)

    def mount(self, root_dir=None, parent_tag_id=None):
        """ Generate HyperTagFS: tag representation using symlinks """
        if parent_tag_id is None:
            print("Updating HyperTagFS...")
            graph()
            tag_ids_names = self.db.get_root_tag_ids_names()
        else:
            tag_ids_names = self.db.get_tag_id_children_ids_names(parent_tag_id)

        leaf_tag_ids = {tag_id[0] for tag_id in self.db.get_leaf_tag_ids()}
        if root_dir is None:
            root_dir = self.root_dir
        root_path = Path(root_dir)
        for tag_id, name in tag_ids_names:
            file_paths_names = self.db.get_file_paths_names_by_tag_id(tag_id)
            root_tag_path = root_path / name
            symlink_path = root_tag_path
            if tag_id not in leaf_tag_ids:
                os.makedirs(root_tag_path / "_files", exist_ok=True)
                symlink_path = root_tag_path / "_files"
            elif len(file_paths_names) > 0:
                os.makedirs(root_tag_path, exist_ok=True)
            for file_path, file_name in file_paths_names:
                try:
                    os.symlink(Path(file_path), symlink_path / file_name)
                except FileExistsError:
                    pass
            self.mount(root_tag_path, tag_id)

    def import_tags(self, import_path: str):
        """Import files with tags inferred from existing directory hierarchy
        (ignores hidden directories)"""
        file_paths = [p for p in list(Path(import_path).rglob("*")) if p.is_file()]
        # Remove files in hidden directories or in ignore list
        ignore_list = set(self.db.get_ignore_list())
        visible_file_paths = []
        for p in file_paths:
            is_hidden = False
            for pp in str(p).split("/"):
                if pp.startswith(".") or pp in ignore_list:
                    is_hidden = True
            if not is_hidden:
                visible_file_paths.append(p)
        print("Adding files...")
        self.add(*visible_file_paths)
        import_path_dirs = set(str(import_path).split("/"))
        print("Adding tags...")
        for file_path in tqdm(visible_file_paths):
            file_path_tags = [p for p in str(file_path).split("/") if p not in import_path_dirs][
                :-1
            ]
            self.tag(
                file_path.name,
                "with",
                *file_path_tags,
                remount=False,
                add=False,
                commit=False,
            )
            for previous, current in zip(file_path_tags, file_path_tags[1:]):
                self.metatag(current, "with", previous, remount=False, commit=False)
            # print(file_path_tags, file_path.name)
        self.db.conn.commit()
        self.mount(self.root_dir)

    def add(self, *file_paths):
        """ Add file/s """
        added = 0
        for file_path in tqdm(file_paths):
            try:
                if Path(file_path).is_file():
                    self.db.add_file(os.path.abspath(file_path))
                    added += 1
            except sqlite3.IntegrityError:
                pass
        self.db.conn.commit()
        print("Added", added, "new file/s")

    def tags(self, *file_names):
        """ Display all tags of file/s """
        tags = set()
        for file_name in file_names:
            tags.update(set(self.db.get_tags_by_file_name(file_name)))
        for tag in tags:
            print(tag)

    def show(self, mode="tags", path=False):
        """ Display all tags (default), indexed files (mode=index) or files """
        if mode == "files":
            names = self.db.get_files(path)
        elif mode == "index":
            names = self.db.get_indexed_file_paths(path)
        elif mode == "tags":
            names = self.db.get_tags()
        for name in names:
            print(name)

    def query(self, *query, path=False, fuzzy=True):
        """Query files using set operands.
        Supported operands:
        and (intersection (default))
        or (union)
        minus (difference)
        """
        # TODO: Parse AST to support queries with brackets
        operands = {"and", "or", "minus"}
        results = set(self.db.get_files_by_tag(query[0], path, fuzzy=fuzzy))
        current_operand = None
        for query_symbol in query[1:]:
            if query_symbol not in operands:
                file_names = set(self.db.get_files_by_tag(query_symbol, path, fuzzy=fuzzy))
                if current_operand == "or":
                    results = results.union(file_names)
                elif current_operand == "minus":
                    results = results - file_names
                else:
                    # and (intersection) is default operand
                    results = results.intersection(file_names)
            else:
                current_operand = query_symbol
        return results

    def tag(self, *args, remount=True, add=True, commit=True):
        """ Tag file/s with tag/s """
        # Parse arguments
        file_names = []
        tags = []
        is_file_name = True
        for arg in args:
            if arg == "with":
                is_file_name = False
                continue
            if is_file_name:
                file_names.append(arg)
            else:
                tags.append(arg)
        if add:
            self.add(*file_names)
        # Add tags
        for file_name in file_names:
            file_name = file_name.split("/")[-1]
            for tag in tags:
                self.db.add_tag_to_file(tag, file_name)
            # print("Tagged", file_name, "with", tags)
        if commit:
            self.db.conn.commit()
        # Remount (everything is mounted)
        if remount:
            self.mount(self.root_dir)

    def untag(self, *args, remount=True, commit=True):
        """ Untag (remove tag/s of) file/s with tag/s """
        # Parse arguments
        file_names = []
        tags = []
        is_file_name = True
        for arg in args:
            if arg == "with":
                is_file_name = False
                continue
            if is_file_name:
                file_names.append(arg)
            else:
                tags.append(arg)
        # Remove tags
        for file_name in file_names:
            file_name = file_name.split("/")[-1]
            for tag in tags:
                self.db.remove_tag_from_file(tag, file_name)
            # print("Tagged", file_name, "with", tags)
        if commit:
            self.db.conn.commit()
        # Remount (everything is mounted)
        if remount:
            self.mount(self.root_dir)

    def metatags(self, *tag_names):
        """ Display all metatags of tag/s """
        tags = set()
        for tag_name in tag_names:
            tags.update(set(self.db.get_meta_tags_by_tag_name(tag_name)))
        for tag in tags:
            print(tag)

    def metatag(self, *args, remount=True, commit=True):
        """ Tag tag/s with tag/s """
        # Parse arguments
        parent_tags = []
        tags = []
        is_parent_tag = False
        for arg in args:
            if arg == "with":
                is_parent_tag = True
                continue
            if is_parent_tag:
                parent_tags.append(arg)
            else:
                tags.append(arg)

        # Add tags
        for tag in tags:
            for parent_tag in parent_tags:
                self.db.add_parent_tag_to_tag(parent_tag, tag)
            # print("MetaTagged", tag, "with", parent_tags)
        if commit:
            self.db.conn.commit()

        for tag in tags:
            for parent_tag in parent_tags:
                # Remove parent_tag dir in all levels
                self.rmdir(self.root_dir, parent_tag)
                # Remove tag dir in root level
                try:
                    rmtree(self.root_dir / tag)
                except Exception:  # nosec
                    pass  # Ignore if non existing
        # Remount (everything is mounted)
        if remount:
            self.mount(self.root_dir)

    def rmdir(self, directory, del_dir_name):
        # Delete dirs named del_dir_name in directory recursively
        for item in directory.iterdir():
            if item.name == del_dir_name and item.is_dir():
                rmtree(item)
            if item.is_dir():
                self.rmdir(item, del_dir_name)

    def merge(self, tag_a, _into, tag_b):
        """ Merges all associations (files & tags) of tag_a into tag_b """
        print("Merging tag", tag_a, "into", tag_b)
        self.db.merge_tags(tag_a, tag_b)
        self.rmdir(self.root_dir, tag_a)
        self.mount(self.root_dir)


def daemon():
    """ Start daemon process watching HyperTagFS """
    p = Process(target=start)
    p.start()
    p.join()


def help():
    """ Get some help on how to use HyperTag """
    print(
        """
    Found a bug or simply feeling lost?
    Do not despair, you're not alone! Let us know at https://github.com/SeanPedersen/HyperTag/issues
    Print available CLI functions: hypertag
    Print available CLI flags for all commands: hypertag command --help
    """
    )


def main():
    ht = HyperTag()
    fire_cli = {
        "help": help,
        "add": ht.add,
        "import": ht.import_tags,
        "tag": ht.tag,
        "untag": ht.untag,
        "metatag": ht.metatag,
        "merge": ht.merge,
        "show": ht.show,
        "tags": ht.tags,
        "metatags": ht.metatags,
        "query": ht.query,
        "set_hypertagfs_dir": ht.set_hypertagfs_dir,
        "mount": ht.mount,
        "daemon": daemon,
        "graph": graph,
        "index": ht.index,
        "search": ht.search,
    }
    fire.Fire(fire_cli)


if __name__ == "__main__":
    main()

import time
import os
from shutil import rmtree
import sqlite3
from pathlib import Path
import fire
from tqdm import tqdm
import filetype


class Persistor:
    """ SQLite3 DB persistence class """

    def __init__(self):
        path = Path(__file__).parent.parent / "hypertag.db"
        self.hypertagfs_dir = "hypertagfs_dir"
        self.hypertagfs_name = "HyperTagFS"
        self.ignore_list_name = "ignore_list"
        self.ignore_list = ["node_modules", "__pycache__"]
        self.conn = sqlite3.connect(path)
        self.c = self.conn.cursor()
        self.file_groups_types = {
            "Images": ["jpg", "png", "svg", "tif", "ico", "icns"],
            "Videos": ["mp4", "gif", "webm", "avi", "mkv"],
            "Documents": ["txt", "md", "rst", "pdf", "epub", "doc", "docx"],
            "Source Code": ["sh", "py", "pyx", "ipynb", "c", "h", "cpp", "rs", "erl", "ex", "js", "ts", "css", "html", "sql"],
            "Configs": ["yml", "xml", "conf", "ini", "toml", "json", "lock"],
            "Archives": ["zip", "gz", "xz", "z", "sz", "lz", "bz2", "tar", "iso", "7z", "rar"],
            "Blobs": ["bin", "pyc", "so", "o", "ar", "a", "lib", "rmeta", "jar", "exe"],
            "Misc": ["ll", "d", "tag", "blend", "map"],
        }
        self.file_types_groups = dict()
        for group, types in self.file_groups_types.items():
            for file_type in types:
                self.file_types_groups[file_type] = group

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            meta(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                value TEXT
            )
            """
        )
        self.c.executemany(
            """
            INSERT OR IGNORE INTO meta(
                name,
                value
            )
            VALUES(?, ?)
            """,
            [(self.hypertagfs_dir, str(Path.home() / self.hypertagfs_name)),
            (self.ignore_list_name, ",".join(self.ignore_list))]
        )

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            files(
                file_id INTEGER PRIMARY KEY,
                name TEXT,
                path TEXT UNIQUE
            )
            """
        )

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            tags(
                tag_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            )
            """
        )

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            tags_files(
                tag_id INTEGER,
                file_id INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(tag_id),
                FOREIGN KEY (file_id) REFERENCES files(file_id),
                PRIMARY KEY (tag_id, file_id)
            )
            """
        )

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            tags_tags(
                parent_tag_id INTEGER,
                children_tag_id INTEGER,
                FOREIGN KEY (parent_tag_id) REFERENCES tags(tag_id),
                FOREIGN KEY (children_tag_id) REFERENCES tags(tag_id),
                PRIMARY KEY (parent_tag_id, children_tag_id)
            )
            """
        )

        for group, types in self.file_groups_types.items():
            self.add_tag(group)
            for file_type in types:
                self.add_tag(file_type)
                self.add_parent_tag_to_tag(group, file_type)

    def close(self):
        self.conn.commit()
        self.c.close()
        self.conn.close()

    def __enter__(self):
        # Needed for the Destructor
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Destructor: closes connection and cursor """
        self.close()

    def set_hypertagfs_dir(self, path: str):
        self.c.execute(
            """
            UPDATE meta
            SET
                value = ?
            WHERE
                name = ?
            """,
            [str(Path(path) / self.hypertagfs_name), self.hypertagfs_dir],
        )
        self.conn.commit()

    def get_hypertagfs_dir(self):
        self.c.execute(
            """
            SELECT value
            FROM meta
            WHERE name = ?
            """,
            (self.hypertagfs_dir,)
        )
        data = self.c.fetchone()[0]
        return data

    def get_ignore_list(self):
        self.c.execute(
            """
            SELECT value
            FROM meta
            WHERE name = ?
            """,
            (self.ignore_list_name,)
        )
        data = self.c.fetchone()[0].split(",")
        return data

    def add_file(self, path: str):
        file_path = Path(path)
        file_name = file_path.name
        file_type_guess = filetype.guess(str(file_path))
        if file_type_guess is None:
            file_name_splits = file_name.split(".")
            candidate_file_type = file_name_splits[-1].lower()
            if len(file_name_splits) > 1 and len(candidate_file_type) < 7:
                file_type = candidate_file_type
            else:
                file_type = None
        else:
            file_type = file_type_guess.extension#

        self.c.execute(
            """
            INSERT INTO files(
                name,
                path
            )
            VALUES(?, ?)
            """,
            (file_name, str(file_path)),
        )
        if len(file_name.split(".")) > 1 and file_type:
            self.add_tag_to_file(file_name, file_type)
        file_group = self.file_types_groups.get(file_type)
        if file_group:
            self.add_tag_to_file(file_name, file_group)

    def add_tag(self, name: str):
        self.c.execute(
            """
            INSERT OR IGNORE INTO tags(
                name
            )
            VALUES(?)
            """,
            [name],
        )
        self.c.execute(
            """
            SELECT tag_id FROM tags WHERE name LIKE ?
            """,
            [name],
        )
        tag_id = self.c.fetchone()[0]
        return tag_id

    def get_files(self):
        self.c.execute(
            """
            SELECT name
            FROM files
            """
        )
        data = self.c.fetchall()
        return data

    def add_tag_to_file(self, file_name: str, tag_name: str):
        self.c.execute(
            """
            INSERT OR IGNORE INTO tags(
                name
            )
            VALUES(?)
            """,
            [tag_name],
        )
        self.c.execute(
            """
            SELECT tag_id FROM tags WHERE name LIKE ?
            """,
            [tag_name],
        )
        tag_id = self.c.fetchone()[0]

        self.c.execute(
            """
            SELECT file_id FROM files WHERE name LIKE ?
            """,
            [file_name],
        )
        file_id = self.c.fetchone()[0]

        self.c.execute(
            """
            INSERT OR IGNORE INTO tags_files(
                file_id,
                tag_id
            )
            VALUES(?, ?)
            """,
            (file_id, tag_id),
        )

    def get_tag_id_by_name(self, name):
        self.c.execute(
            """
            SELECT tag_id FROM tags WHERE name LIKE ?
            """,
            [name],
        )
        return self.c.fetchone()[0]

    def add_parent_tag_to_tag(self, parent_tag_name: str, tag_name: str):
        self.c.execute(
            """
            INSERT OR IGNORE INTO tags(
                name
            )
            VALUES(?)
            """,
            [parent_tag_name],
        )

        parent_tag_id = self.get_tag_id_by_name(parent_tag_name)
        tag_id = self.get_tag_id_by_name(tag_name)

        self.c.execute(
            """
            INSERT OR IGNORE INTO tags_tags(
                parent_tag_id,
                children_tag_id
            )
            VALUES(?, ?)
            """,
            (parent_tag_id, tag_id),
        )

    def merge_tags(self, tag_a, tag_b):
        tag_a_id = self.get_tag_id_by_name(tag_a)
        tag_b_id = self.get_tag_id_by_name(tag_b)
        # Replace tag file associations
        self.c.execute(
            """
            UPDATE OR IGNORE tags_files
            SET
                tag_id = ?
            WHERE
                tag_id = ?
            """,
            [tag_b_id, tag_a_id],
        )
        # Replace metatag associations
        self.c.execute(
            """
            UPDATE OR IGNORE tags_tags
            SET
                parent_tag_id = ?
            WHERE
                parent_tag_id = ?
            """,
            [tag_b_id, tag_a_id],
        )
        self.c.execute(
            """
            UPDATE OR IGNORE tags_tags
            SET
                children_tag_id = ?
            WHERE
                children_tag_id = ?
            """,
            [tag_b_id, tag_a_id],
        )
        # Delete tag_a
        self.c.execute("DELETE FROM tags_files WHERE tag_id = ?", [tag_a_id])
        self.c.execute("DELETE FROM tags_tags WHERE parent_tag_id = ?", [tag_a_id])
        self.c.execute("DELETE FROM tags_tags WHERE children_tag_id = ?", [tag_a_id])
        self.c.execute("DELETE FROM tags WHERE tag_id = ?", [tag_a_id])
        self.conn.commit()

    def get_root_tag_ids_names(self):
        self.c.execute(
            """
            SELECT tag_id, name FROM tags WHERE tag_id NOT IN
            (SELECT children_tag_id FROM tags_tags)
            """
        )
        data = self.c.fetchall()
        return data
    
    def get_leaf_tag_ids(self):
        self.c.execute(
            """
            SELECT tag_id, name FROM tags WHERE tag_id NOT IN
            (SELECT parent_tag_id FROM tags_tags)
            """
        )
        data = self.c.fetchall()
        return data

    def get_tag_id_children_ids_names(self, parent_tag_id):
        self.c.execute(
            """
            SELECT tag_id, name FROM tags, tags_tags
            WHERE tag_id = children_tag_id AND parent_tag_id = ?
            """,
            (parent_tag_id,)
        )
        data = self.c.fetchall()
        return data

    def get_tags(self):
        self.c.execute(
            """
            SELECT name
            FROM tags
            """
        )
        data = self.c.fetchall()
        return data
    
    def get_tags_by_file_name(self, file_name):
        self.c.execute(
            """
            SELECT t.name
            FROM tags as t, tags_files as tf, files as f
            WHERE 
                t.tag_id = tf.tag_id AND
                tf.file_id = f.file_id AND
                f.name LIKE ?
            """,
            (file_name,)
        )
        data = [e[0] for e in self.c.fetchall()]
        return data

    def get_file_paths_names_by_tag_id(self, tag_id):
        self.c.execute(
            """
            SELECT f.path, f.name
            FROM files f, tags t, tags_files tf
            WHERE f.file_id = tf.file_id AND
                tf.tag_id = t.tag_id AND
                t.tag_id = ?
            """,
            [tag_id],
        )
        data = self.c.fetchall()
        return data

    def get_files_by_tag(self, tag_name):
        self.c.execute(
            """
            SELECT f.name
            FROM files f, tags t, tags_files tf
            WHERE f.file_id = tf.file_id AND
                tf.tag_id = t.tag_id AND
                t.name LIKE ?
            """,
            [tag_name],
        )
        data = [e[0] for e in self.c.fetchall()]
        return data


class HyperTag():
    """ HyperTag CLI """
    def __init__(self):
        self._db = Persistor()
        self.root_dir = Path(self._db.get_hypertagfs_dir())
        os.makedirs(self.root_dir, exist_ok=True)

    def set_hypertagfs_dir(self, path):
        """ Set path for HyperTagFS directory """
        self._db.set_hypertagfs_dir(path)

    def mount(self, root_dir, parent_tag_id=None):
        """ Add file system tag representation using symlinks """
        if parent_tag_id is None:
            print("Building HyperTagFS...")
            rmtree(self.root_dir)
            tag_ids_names = self._db.get_root_tag_ids_names()
        else:
            tag_ids_names = self._db.get_tag_id_children_ids_names(parent_tag_id)

        leaf_tag_ids = {tag_id[0] for tag_id in self._db.get_leaf_tag_ids()}
        root_path = Path(root_dir)
        for tag_id, name in tag_ids_names:
            file_paths_names = self._db.get_file_paths_names_by_tag_id(tag_id)
            root_tag_path = root_path / name
            symlink_path = root_tag_path
            if tag_id not in leaf_tag_ids:
                os.makedirs(root_tag_path / "_files", exist_ok=True)
                symlink_path = (root_tag_path  / "_files")
            elif len(file_paths_names) > 0:
                os.makedirs(root_tag_path, exist_ok=True)
            for file_path, file_name in file_paths_names:
                try:
                    os.symlink(Path(file_path), symlink_path / file_name)
                except FileExistsError as _ex:
                    pass
            self.mount(root_tag_path, tag_id)

    def import_tags(self, import_path):
        """ Imports files with tags from existing directory hierarchy (ignores hidden directories) """
        file_paths = [p for p in list(Path(import_path).rglob("*")) if p.is_file()]
        # Remove files in hidden directories or in ignore list
        ignore_list = set(self._db.get_ignore_list())
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
            file_path_tags = [p for p in str(file_path).split("/") if p not in import_path_dirs][:-1]
            self.tag(file_path.name, "with", *file_path_tags, remount=False, add=False, commit=False)
            for previous, current in zip(file_path_tags, file_path_tags[1:]):
                self.metatag(current, "with", previous, remount=False, commit=False)
            #print(file_path_tags, file_path.name)
        self._db.conn.commit()
        self.mount(self.root_dir)

    def add(self, *file_paths):
        """ Add file/s """
        added = 0
        for file_path in tqdm(file_paths):
            try:
                if Path(file_path).is_file():
                    self._db.add_file(os.path.abspath(file_path))
                    added += 1
            except sqlite3.IntegrityError as _ex:
                pass
        self._db.conn.commit()
        print("Added", added, "new file/s")

    def tags(self, *file_names):
        """ Display all tags of file/s """
        tags = set()
        for file_name in file_names:
            tags.update(set(self._db.get_tags_by_file_name(file_name)))
        for tag in tags:
            print(tag)

    def show(self, mode="tags"):
        """ Display all tags (default) or files """
        if mode == "files":
            names = self._db.get_files()
        elif mode == "tags":
            names = self._db.get_tags()
        for name in names:
            print(name[0])

    def query(self, *query):
        """ Query files using set operands.
            Supported operands:
              - and : intersection (default)
              - or : union
              - minus : difference
        """
        # TODO: Parse AST to support queries with brackets
        operands = {"and", "or", "minus"}
        results = set(self._db.get_files_by_tag(query[0]))
        current_operand = None
        for query_symbol in query[1:]:
            if query_symbol not in operands:
                file_names = set(self._db.get_files_by_tag(query_symbol))                    
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
                self._db.add_tag_to_file(file_name, tag)
            #print("Tagged", file_name, "with", tags)
        if commit:
            self._db.conn.commit()
        # Remount (everything is mounted TODO: make it lazy)
        if remount:
            self.mount(self.root_dir)

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
                self._db.add_parent_tag_to_tag(parent_tag, tag)
            #print("MetaTagged", tag, "with", parent_tags)
        if commit:
            self._db.conn.commit()
        # Remount (everything is mounted TODO: make it lazy)
        if remount:
            self.mount(self.root_dir)

    def merge(self, tag_a, _into, tag_b):
        """ Merges all associations (files & tags) of tag_a into tag_b """
        print("Merging tag", tag_a, "into", tag_b)
        self._db.merge_tags(tag_a, tag_b)
        self.mount(self.root_dir)

if __name__ == '__main__':
    ht = HyperTag()
    fire_cli = {
        "add": ht.add,
        "import": ht.import_tags,
        "tag": ht.tag,
        "metatag": ht.metatag,
        "merge": ht.merge,
        "show": ht.show,
        "tags": ht.tags,
        "query": ht.query,
        "set_hypertagfs_dir": ht.set_hypertagfs_dir
    }
    fire.Fire(fire_cli)

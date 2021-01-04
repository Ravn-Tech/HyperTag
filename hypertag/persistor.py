import os
import sqlite3
from pathlib import Path
import filetype  # type: ignore


class Persistor:
    """ SQLite3 DB persistence class """

    def __init__(self):
        db_path = Path.home() / ".config/hypertag/"
        os.makedirs(db_path, exist_ok=True)
        path = db_path / "hypertag.db"
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
            "Source Code": [
                "sh",
                "py",
                "pyx",
                "ipynb",
                "c",
                "h",
                "cpp",
                "rs",
                "erl",
                "ex",
                "js",
                "ts",
                "css",
                "html",
                "sql",
            ],
            "Configs": ["yml", "xml", "conf", "ini", "toml", "json", "lock"],
            "Archives": [
                "zip",
                "gz",
                "xz",
                "z",
                "sz",
                "lz",
                "bz2",
                "tar",
                "iso",
                "7z",
                "rar",
            ],
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
            [
                (self.hypertagfs_dir, str(Path.home() / self.hypertagfs_name)),
                (self.ignore_list_name, ",".join(self.ignore_list)),
            ],
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

        self.conn.commit()

    def close(self):
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
            (self.hypertagfs_dir,),
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
            (self.ignore_list_name,),
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
                file_type = ""
        else:
            file_type = file_type_guess.extension

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
            self.add_tag_to_file(file_type, file_name)
        file_group = self.file_types_groups.get(file_type)
        if file_group:
            self.add_tag_to_file(file_group, file_name)

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
        tag_id = self.get_tag_id_by_name(name)
        return tag_id

    def get_files(self, show_path):
        if show_path:
            self.c.execute("SELECT path FROM files")
        else:
            self.c.execute("SELECT name FROM files")
        data = [e[0] for e in self.c.fetchall()]
        return data

    def add_tag_to_file(self, tag_name: str, file_name: str):
        file_id = self.get_file_id_by_name(file_name)
        tag_id = self.add_tag(tag_name)

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

    def remove_tag_from_file(self, tag_name: str, file_name: str):
        file_id = self.get_file_id_by_name(file_name)
        tag_id = self.get_tag_id_by_name(tag_name)
        self.c.execute("DELETE FROM tags_files WHERE file_id = ? AND tag_id = ?", [file_id, tag_id])
        self.conn.commit()

    def get_tag_name_by_id(self, tag_id: int):
        self.c.execute("SELECT name FROM tags WHERE tag_id = ?", [tag_id])
        return self.c.fetchone()[0]

    def get_tag_id_by_name(self, name: str):
        self.c.execute("SELECT tag_id FROM tags WHERE name LIKE ?", [name])
        return self.c.fetchone()[0]

    def get_file_id_by_name(self, name: str):
        self.c.execute("SELECT file_id FROM files WHERE name LIKE ?", [name])
        return self.c.fetchone()[0]

    def remove_parent_tag_from_tag(self, parent_tag_name: str, tag_name: str):
        try:
            parent_tag_id = self.get_tag_id_by_name(parent_tag_name)
            tag_id = self.get_tag_id_by_name(tag_name)
        except TypeError:
            return  # Abort if a tag is missing
        self.c.execute(
            """
            DELETE FROM tags_tags
            WHERE parent_tag_id = ? AND children_tag_id = ?""",
            [parent_tag_id, tag_id],
        )
        self.conn.commit()

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
        self.add_tag(tag_name)
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

    def remove_tag(self, tag_name):
        # Remove all tag associations
        tag_id = self.get_tag_id_by_name(tag_name)
        self.c.execute("DELETE FROM tags_files WHERE tag_id = ?", [tag_id])
        self.c.execute("DELETE FROM tags_tags WHERE parent_tag_id = ?", [tag_id])
        self.c.execute("DELETE FROM tags_tags WHERE children_tag_id = ?", [tag_id])
        self.c.execute("DELETE FROM tags WHERE tag_id = ?", [tag_id])
        self.conn.commit()

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
        self.remove_tag(tag_a)  # Commits

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
            (parent_tag_id,),
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
        data = [e[0] for e in self.c.fetchall()]
        return data

    def get_meta_tags_by_tag_name(self, tag_name):
        self.c.execute(
            """
            SELECT tt.parent_tag_id
            FROM tags as t, tags_tags as tt
            WHERE 
                tt.children_tag_id = t.tag_id AND
                t.name LIKE ?
            """,
            (tag_name,),
        )
        data = [self.get_tag_name_by_id(e[0]) for e in self.c.fetchall()]
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
            (file_name,),
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

    def get_files_by_tag(self, tag_name, show_path):
        if show_path:
            select = "SELECT f.path"
        else:
            select = "SELECT f.name"
        self.c.execute(
            select
            + """
            FROM files f, tags t, tags_files tf
            WHERE f.file_id = tf.file_id AND
                tf.tag_id = t.tag_id AND
                t.name LIKE ?
            """,
            [tag_name],
        )
        data = [e[0] for e in self.c.fetchall()]
        return data

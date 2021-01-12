import os
import sqlite3
from typing import List
from pathlib import Path
import filetype  # type: ignore
from fuzzywuzzy import process  # type: ignore


class Persistor:
    """ SQLite3 DB persistence class """

    def __init__(self):
        self.db_path = Path.home() / ".config/hypertag/"
        os.makedirs(self.db_path, exist_ok=True)
        path = self.db_path / "hypertag.db"
        self.hypertagfs_dir = "hypertagfs_dir"
        self.hypertagfs_name = "HyperTagFS"
        self.ignore_list_name = "ignore_list"
        self.ignore_list = ["node_modules", "__pycache__"]
        self.conn = sqlite3.connect(str(path))
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
                name TEXT NOT NULL,
                path TEXT UNIQUE NOT NULL,
                embedding_vector TEXT,
                clean_text TEXT
            )
            """
        )

        # Migrate old files table TODO: Remove on 1.0 release (add to CREATE)
        try:
            self.c.execute("ALTER TABLE files ADD COLUMN embedding_vector TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            self.c.execute("ALTER TABLE files ADD COLUMN clean_text TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            self.c.execute("ALTER TABLE files ADD COLUMN indexed INTEGER")
        except sqlite3.OperationalError:
            pass

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            tags(
                tag_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
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
        # Migrate old tags_files table TODO: Remove on 1.0 release (add to CREATE)
        try:
            self.c.execute("ALTER TABLE tags_files ADD COLUMN value TEXT")
        except sqlite3.OperationalError:
            pass

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

        self.c.execute(
            """
            CREATE TABLE IF NOT EXISTS
            auto_import_directories(
                id INTEGER PRIMARY KEY,
                path TEXT UNIQUE
            )
            """
        )
        try:
            self.c.execute(
                "ALTER TABLE auto_import_directories ADD COLUMN auto_index_images INTEGER"
            )
            self.c.execute(
                "ALTER TABLE auto_import_directories ADD COLUMN auto_index_texts INTEGER"
            )
        except sqlite3.OperationalError:
            pass

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
            self.add_tag_to_file(file_type, str(file_path))
        file_group = self.file_types_groups.get(file_type)
        if file_group:
            self.add_tag_to_file(file_group, str(file_path))

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

    def add_auto_import_directory(self, path: str, auto_index_images: bool, auto_index_text: bool):
        path = str(Path(path))  # Standardizes path format (trailing / etc.)
        self.c.execute(
            """
            INSERT OR REPLACE INTO auto_import_directories(
                path,
                auto_index_images,
                auto_index_texts
            )
            VALUES(?, ?, ?)
            """,
            [path, int(auto_index_images), int(auto_index_text)],
        )
        auto_import_dir_id = self.get_auto_import_id_by_path(path)
        self.conn.commit()
        return auto_import_dir_id

    def get_auto_index_images(self, path):
        self.c.execute(
            "SELECT auto_index_images FROM auto_import_directories WHERE path LIKE ?", [path]
        )
        return bool(self.c.fetchone()[0])

    def get_auto_index_texts(self, path):
        self.c.execute(
            "SELECT auto_index_texts FROM auto_import_directories WHERE path LIKE ?", [path]
        )
        return bool(self.c.fetchone()[0])

    def update_file_by_id(self, file_id: int, file_path: str):
        file_path = str(file_path)
        file_name = file_path.split("/")[-1]
        self.c.execute(
            """
            UPDATE files
            SET
                name = ?,
                path = ?
            WHERE
                file_id = ?
            """,
            [file_name, file_path, file_id],
        )
        self.conn.commit()

    def add_file_embedding_vector(self, file_path: str, embedding_vector: str):
        self.c.execute(
            """
            UPDATE OR IGNORE files
            SET
                embedding_vector = ?
            WHERE
                path = ?
            """,
            [str(embedding_vector), file_path],
        )

    def get_file_embedding_vectors(self, file_paths):
        # Returns list of (file_id, embedding_vector)
        file_embedding_vectors = []
        for file_path in file_paths:
            self.c.execute(
                """
                SELECT path, embedding_vector
                FROM files
                WHERE embedding_vector IS NOT NULL AND
                    path = ?
                """,
                (str(file_path),),
            )
            result = self.c.fetchone()
            if result:
                path, embedding_vector = result
                if embedding_vector != "nan":
                    file_embedding_vectors.append((path, embedding_vector))
        return file_embedding_vectors

    def set_indexed_by_file_paths(self, file_paths: List[str]):
        for file_path in file_paths:
            self.c.execute(
                """
                UPDATE files
                SET
                    indexed = 1
                WHERE
                    path = ?
                """,
                [file_path],
            )
        self.conn.commit()

    def get_unindexed_file_paths(self, show_path=True):
        if show_path:
            head = "SELECT path"
        else:
            head = "SELECT name"
        self.c.execute(head + " FROM files WHERE indexed is NULL")
        data = [e[0] for e in self.c.fetchall()]
        return data

    def get_vectorized_file_paths(self, show_path=True):
        if show_path:
            head = "SELECT path"
        else:
            head = "SELECT name"
        self.c.execute(head + " FROM files WHERE embedding_vector is NOT NULL")
        data = [e[0] for e in self.c.fetchall()]
        return data

    def get_unvectorized_file_paths(self) -> List[str]:
        self.c.execute(
            "SELECT path FROM files WHERE embedding_vector is NULL AND clean_text is NULL"
        )
        data = [str(e[0]) for e in self.c.fetchall()]
        return data

    def get_files(self, show_path):
        if show_path:
            self.c.execute("SELECT path FROM files")
        else:
            self.c.execute("SELECT name FROM files")
        data = [e[0] for e in self.c.fetchall()]
        return data

    def add_tag_to_file(self, tag_name: str, file_path: str, value=None):
        if "/" in file_path:
            file_id = self.get_file_id_by_path(file_path)
        else:
            file_id = self.get_file_id_by_name(file_path)
        tag_id = self.add_tag(tag_name)

        self.c.execute(
            """
            INSERT OR IGNORE INTO tags_files(
                file_id,
                tag_id,
                value
            )
            VALUES(?, ?, ?)
            """,
            (file_id, tag_id, value),
        )

    def remove_tag_from_file(self, tag_name: str, file_name: str):
        try:
            file_id = self.get_file_id_by_name(file_name)
            tag_id = self.get_tag_id_by_name(tag_name)
        except TypeError:
            return
        self.c.execute("DELETE FROM tags_files WHERE file_id = ? AND tag_id = ?", [file_id, tag_id])
        self.conn.commit()

    def get_tag_name_by_id(self, tag_id: int):
        self.c.execute("SELECT name FROM tags WHERE tag_id = ?", [tag_id])
        return self.c.fetchone()[0]

    def get_tag_id_by_name(self, name: str):
        self.c.execute("SELECT tag_id FROM tags WHERE name LIKE ?", [name])
        return self.c.fetchone()[0]

    def get_file_id_by_path(self, path: str):
        self.c.execute("SELECT file_id FROM files WHERE path LIKE ?", [path])
        path = self.c.fetchone()
        if path:
            path = path[0]
        return path

    def get_file_id_by_name(self, name: str):
        self.c.execute("SELECT file_id FROM files WHERE name LIKE ?", [name])
        return self.c.fetchone()[0]

    def get_file_path_by_id(self, file_id: int):
        self.c.execute("SELECT file_path FROM files WHERE file_id = ?", [file_id])
        return self.c.fetchone()[0]

    def get_auto_import_id_by_path(self, path: str):
        self.c.execute("SELECT id FROM auto_import_directories WHERE path LIKE ?", [path])
        return self.c.fetchone()[0]

    def get_auto_import_paths(self):
        self.c.execute("SELECT path FROM auto_import_directories")
        return [e[0] for e in self.c.fetchall()]

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

    def get_clean_text_of_file(self, file_path: str):
        self.c.execute(
            """
            SELECT clean_text FROM files WHERE path LIKE ?
            """,
            (file_path,),
        )
        data = self.c.fetchone()[0]
        return data

    def add_clean_text_to_file(self, file_path: str, clean_text: str):
        self.c.execute(
            """
            UPDATE files
            SET
                clean_text = ?
            WHERE
                path = ?
            """,
            [clean_text, file_path],
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

    def remove_file(self, file_name):
        file_id = self.get_file_id_by_name(file_name)
        self.c.execute("DELETE FROM tags_files WHERE file_id = ?", [file_id])
        self.c.execute("DELETE FROM files WHERE file_id = ?", [file_id])
        self.conn.commit()

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

    def get_file_paths_names_by_tag_id_shallow(self, tag_id):
        # Includes all files of tag id
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

    def get_file_paths_names_by_tag_id(self, tag_id, files_data=[]):
        # Recursively includes all children tags files
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
        data = self.c.fetchall() + files_data

        children_tuples = self.get_tag_id_children_ids_names(tag_id)
        for child_tag_id, _name in children_tuples:
            data = self.get_file_paths_names_by_tag_id(child_tag_id, data)
        return data

    def get_files_by_tag(self, tag_name, show_path, fuzzy, value=None, verbose=False):
        if fuzzy:
            matches = process.extract(tag_name, self.get_tags(), limit=5)
            best_match = None, None
            best_dist = float("inf")
            for match, ratio in matches:
                length_dist = abs(len(match) - len(tag_name)) + 1
                length_overlap = len(set(tag_name).intersection(match))
                dist = ((length_dist ** 0.5) / ((length_overlap + 1) ** 3)) / ratio
                if dist < best_dist:
                    best_dist = dist
                    best_match = match, ratio

            tag_name, ratio = best_match
            if verbose:
                print(f"Fuzzy matched: {tag_name} ({best_dist*100:.5f})")
        if show_path:
            select = "SELECT f.path"
        else:
            select = "SELECT f.name"
        value_q = ""
        bindings = [tag_name]
        if value:
            value_q = "AND tf.value LIKE ?"
            bindings = [tag_name, value]
        self.c.execute(
            select
            + """
            FROM files f, tags t, tags_files tf
            WHERE f.file_id = tf.file_id AND
                tf.tag_id = t.tag_id AND
                t.name LIKE ?
            """
            + value_q,
            bindings,
        )
        data = [e[0] for e in self.c.fetchall()]
        return data

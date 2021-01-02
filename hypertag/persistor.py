import sqlite3
from pathlib import Path
import filetype


class Persistor:
    """ SQLite3 DB persistence class """

    def __init__(self):
        path = "./hypertag.db"
        self.hypertagfs_dir = "hypertagfs_dir"
        self.hypertagfs_name = "HyperTagFS"
        self.conn = sqlite3.connect(path)
        self.c = self.conn.cursor()
        self.file_groups_types = {
            "Images": ["jpg", "png", "svg"],
            "Documents": ["txt", "pdf", "epub", "doc", "docx"],
            "Source Code": ["py", "ipynb", "c", "cpp", "rs", "erl", "ex", "js", "ts", "css", "html"]
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
        self.c.execute(
            """
            INSERT OR IGNORE INTO meta(
                name,
                value
            )
            VALUES(?, ?)
            """,
            (self.hypertagfs_dir, str(Path("./") / self.hypertagfs_name)),
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
        self.conn.commit()

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
        self.conn.commit()
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
        self.c.execute(
            """
            SELECT tag_id FROM tags WHERE name LIKE ?
            """,
            [parent_tag_name],
        )
        parent_tag_id = self.c.fetchone()[0]

        self.c.execute(
            """
            SELECT tag_id FROM tags WHERE name LIKE ?
            """,
            [tag_name],
        )
        tag_id = self.c.fetchone()[0]

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
    
    def get_leave_tag_ids(self):
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
        data = self.c.fetchall()
        return data

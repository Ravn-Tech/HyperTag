import sqlite3
from pathlib import Path


class Persistor:
    """ SQLite3 DB persistence class """

    def __init__(self):
        path = "./hypertag.db"
        self.conn = sqlite3.connect(path)
        self.c = self.conn.cursor()

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
            file_templates(
                file_id INTEGER PRIMARY KEY,
                creation_time INTEGER,
                last_edit_time INTEGER,
                name TEXT UNIQUE,
                content TEXT
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

    def add_file(self, path: str):
        file_path = Path(path)
        file_name = file_path.name

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
        self.conn.commit()
    
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

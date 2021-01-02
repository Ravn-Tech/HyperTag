import time
import os
from shutil import rmtree
import sqlite3
from pathlib import Path
import fire
from tqdm import tqdm


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


class HyperTag():
    """ Main class describing the CLI """
    def __init__(self):
        self._db = Persistor()
        self.root_dir = Path("./tagfs")
        os.makedirs(self.root_dir, exist_ok=True)

    def mount(self, root_dir = "tagfs", parent_tag_id=None):
        """ Add file system tag representation using symlinks """
        if parent_tag_id is None:
            rmtree(self.root_dir)
            tag_ids_names = self._db.get_root_tag_ids_names()
        else:
            tag_ids_names = self._db.get_tag_id_children_ids_names(parent_tag_id)

        root_path = Path(root_dir)
        for tag_id, name in tag_ids_names:
            root_tag_path = root_path / name
            os.makedirs(root_tag_path, exist_ok=True)
            os.makedirs(root_tag_path / "files", exist_ok=True)
            for file_path, file_name in self._db.get_file_paths_names_by_tag_id(tag_id):
                try:
                    os.symlink(Path(file_path), (root_tag_path  / "files") / file_name)
                except FileExistsError as _ex:
                    pass
            self.mount(root_tag_path, tag_id)

    def import_tags(self, import_path):
        """ Imports files with tags from existing directory hierarchy """
        file_paths = list(Path(import_path).rglob("*"))
        file_paths = [p for p in file_paths if not str(p).split("/")[-2].startswith(".")]
        file_paths = [p for p in file_paths if p.is_file()]
        print("Adding files...")
        self.add(*file_paths)
        import_path_dirs = set(str(import_path).split("/"))
        print("Adding tags...")
        for file_path in tqdm(file_paths):
            file_path_tags = [p for p in str(file_path).split("/") if p not in import_path_dirs][:-1]
            self.tag(file_path.name, "with", *file_path_tags, remount=False)
            for previous, current in zip(file_path_tags, file_path_tags[1:]):
                self.metatag(current, "with", previous, remount=False)
            #print(file_path_tags, file_path.name)
        self.mount(self.root_dir)

    def add(self, *file_paths):
        """ Add file/s """
        added = 0
        for file_path in tqdm(file_paths):
            try:
                if file_path.is_file():
                    self._db.add_file(os.path.abspath(file_path))
                    added += 1
            except sqlite3.IntegrityError as _ex:
                pass
        print("Added", added, "new file/s")

    def show(self, mode):
        """ Display files or tags """
        if mode == "files":
            names = self._db.get_files()
        elif mode == "tags":
            names = self._db.get_tags()
        for name in names:
            print(name[0])
        if not names:
            print("Nothing to show...")

    def find(self, query):
        """ Find files by tag.
            query: string
        """
        print(f"Searching tag... ({query})")
        print("Found:\n")
        for name in self._db.get_files_by_tag(query):
            print(name)

    def tag(self, *args, remount=True):
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
        
        # Add tags
        for file_name in file_names:
            file_name = file_name.split("/")[-1]
            for tag in tags:
                self._db.add_tag_to_file(file_name, tag)
            #print("Tagged", file_name, "with", tags)

        # Remount (everything is mounted TODO: make it lazy)
        if remount:
            self.mount(self.root_dir)

    def metatag(self, *args, remount=True):
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
        
        # Remount (everything is mounted TODO: make it lazy)
        if remount:
            self.mount(self.root_dir)

if __name__ == '__main__':
    ht = HyperTag()
    fire_cli = {
        "add": ht.add,
        "import": ht.import_tags,
        "tag": ht.tag,
        "metatag": ht.metatag,
        "show": ht.show,
        "find": ht.find
    }
    fire.Fire(fire_cli)

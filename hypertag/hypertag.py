import time
import os
from shutil import rmtree
import sqlite3
from pathlib import Path
import fire
from tqdm import tqdm
from hypertag.persistor import Persistor


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
            rmtree(self.root_dir)
            tag_ids_names = self._db.get_root_tag_ids_names()
        else:
            tag_ids_names = self._db.get_tag_id_children_ids_names(parent_tag_id)

        leave_tag_ids = {tag_id[0] for tag_id in self._db.get_leave_tag_ids()}
        root_path = Path(root_dir)
        for tag_id, name in tag_ids_names:
            root_tag_path = root_path / name
            os.makedirs(root_tag_path, exist_ok=True)
            symlink_path = root_tag_path
            if tag_id not in leave_tag_ids:
                os.makedirs(root_tag_path / "_files", exist_ok=True)
                symlink_path = (root_tag_path  / "_files")
            for file_path, file_name in self._db.get_file_paths_names_by_tag_id(tag_id):
                try:
                    os.symlink(Path(file_path), symlink_path / file_name)
                except FileExistsError as _ex:
                    pass
            self.mount(root_tag_path, tag_id)

    def import_tags(self, import_path):
        """ Imports files with tags from existing directory hierarchy """
        file_paths = [p for p in list(Path(import_path).rglob("*")) if p.is_file()]
        # Remove files in hidden directories
        visible_file_paths = []
        for p in file_paths:
            is_hidden = False
            for pp in str(p).split("/"):
                if pp.startswith("."):
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

    def show(self, mode="tags"):
        """ Display files or tags """
        if mode == "files":
            names = self._db.get_files()
        elif mode == "tags":
            names = self._db.get_tags()
        for name in names:
            print(name[0])
        if not names:
            print("Nothing to show...")

    def query(self, query):
        """ Find files by tag.
            query: string
        """
        for name in self._db.get_files_by_tag(query):
            print(name[0])

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

if __name__ == '__main__':
    ht = HyperTag()
    fire_cli = {
        "add": ht.add,
        "import": ht.import_tags,
        "tag": ht.tag,
        "metatag": ht.metatag,
        "show": ht.show,
        "query": ht.query,
        "set_hypertagfs_dir": ht.set_hypertagfs_dir
    }
    fire.Fire(fire_cli)

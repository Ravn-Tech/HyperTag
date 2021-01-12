import urllib.request
from pathlib import Path
import os
from shutil import rmtree
from tqdm import tqdm  # type: ignore


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, b_size=1, t_size=None):
        if t_size is not None:
            self.total = t_size
        self.update(b * b_size - self.n)


def is_int(s: str):
    try:
        int(s)
        return True
    except ValueError:
        return False


def download_url(url, output_path):
    """ Download url with progress bar """
    with DownloadProgressBar(unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


def remove_dir(directory, del_dir_name):
    # Delete dirs named del_dir_name in directory recursively
    for item in directory.iterdir():
        if item.name == del_dir_name and item.is_dir():
            rmtree(item)
        if item.is_dir():
            remove_dir(item, del_dir_name)


def remove_file(directory, file_name):
    # Delete files named file_name in directory recursively
    directory = Path(directory)
    for item in directory.iterdir():
        if item.name == file_name and item.is_file():
            os.remove(item)
        if item.is_dir():
            remove_file(item, file_name)


def remove_symlink(directory, file_name):
    # Delete symlinks named file_name in directory recursively
    directory = Path(directory)
    for item in directory.iterdir():
        if item.name == file_name and item.is_symlink():
            os.remove(item)
        if item.is_dir():
            remove_symlink(item, file_name)


def update_symlink(directory, file_name, new_path):
    # Update symlinks named file_name with new_path in directory recursively
    directory = Path(directory)
    for item in directory.iterdir():
        if item.name == file_name and item.is_symlink():
            temp_link = directory / ("_temp" + file_name)
            os.symlink(new_path, temp_link)
            os.rename(temp_link, directory / file_name)
        if item.is_dir():
            update_symlink(item, file_name, new_path)

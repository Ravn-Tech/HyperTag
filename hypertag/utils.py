import urllib.request
from tqdm import tqdm  # type: ignore


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, b_size=1, t_size=None):
        if t_size is not None:
            self.total = t_size
        self.update(b * b_size - self.n)


def download_url(url, output_path):
    """ Download url with progress bar """
    with DownloadProgressBar(unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)

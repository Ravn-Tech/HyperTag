# HyperTag

File organization made easy. HyperTag let's humans intuitively express how they think about their files using tags.

## Install
`$ pip install hypertag`

## Quickstart
HyperTag offers a slick CLI but more importantly it creates a directory called ```HyperTagFS``` which is a file system based representation of your files and tags using symbolic links and directories.

**Directory Import**: Import your existing directory hierarchies using ```$ hypertag import path/to/directory```. HyperTag converts it automatically into a tag hierarchy using metatagging.

**File Type Groups**: HyperTag automatically creates folders containing common files (e.g. Images: jpg, png, etc., Documents: txt, pdf, etc., Source Code: py, js, etc.), which can be found in ```HyperTagFS```.

**HyperTagFS Daemon  (Experimental)**: Monitors `HyperTagFS` for user changes. Currently supports file and directory (tag) deletions + directory (name as query) creation with automatic query result population.

## CLI Functions

### Start HyperTagFS daemon
Starts process watching HyperTagFS dir for user changes.

```$ hypertag daemon```

### Set HyperTagFS directory path
Default is the user's home directory.

```$ hypertag set_hypertagfs_dir path/to/directory```

### Import existing directory recursively
Import files with tags inferred from existing directory hierarchy

```$ hypertag import path/to/directory```

### Tag file/s
Manually tag files

```$ hypertag tag humans/*.txt with human "Homo Sapiens"```

### Untag file/s
Manually remove tag/s from file/s

```$ hypertag untag humans/*.txt with human "Homo Sapiens"```

### Tag a tag
Metatag tag/s to create tag hierarchies

```$ hypertag metatag human with animal```

### Merge tags
Merges all associations (files & tags) of tag A into tag B

```$ hypertag merge human into "Homo Sapiens"```

### Query using Set Theory
Prints file names matching the query. Nesting is currently not supported, queries are evaluated from left to right.

Print paths: ```$ hypertag query human --path```

Default operand is AND (intersection): <br>
```$ hypertag query human "Homo Sapiens"```

OR (union): <br>
```$ hypertag query human or "Homo Sapiens"```

MINUS (difference): <br>
```$ hypertag query human minus "Homo Sapiens"```

### Print all tags of file/s

```$ hypertag tags filename1 filename2```

### Print all metatags of tag/s

```$ hypertag metatags tag1 tag2```

### Print all tags

```$ hypertag show```

### Print all files

Print names:
```$ hypertag show files```

Print paths:
```$ hypertag show files --path```

## Architecture
- Python powers HyperTag
- SQLite3 serves as the meta data storage engine (located at `~/.config/hypertag/hypertag.db`)
- Symbolic links are used to create the HyperTagFS directory structure

## Development
- Clone repo: ```$ git clone https://github.com/SeanPedersen/HyperTag.git```
- `$ cd HyperTag/`
- Install [Poetry](https://python-poetry.org/docs/#installation)
- Install dependencies: `$ poetry install`
- Activate virtual environment: `$ poetry shell`
- Run all tests: ```$ pytest -v```
- Run Black formatter: ```$ black hypertag/```
- Run PyLint: ```$ pylint **/*.py```
- Run MyPy: ```$ mypy **/*.py```
- Run Bandit: ```$ bandit --exclude tests/ -r .```
- Run HyperTag: ```$ python -m hypertag```

## Inspiration
This project is inspired by other existing open-source projects:
- [TMSU](https://github.com/oniony/TMSU): Written in Go
- [SuperTag](https://github.com/amoffat/supertag): Written in Rust

**What is the point of HyperTag's existence?** HyperTag offers some unique features such as the import function that make it very convenient to use. Also HyperTag's code base is written in Python and thus extremely small (<600 LOC) compared to TMSU (>10,000 LOC) and SuperTag (>25,000 LOC), making it easy to modify / contribute yourself.

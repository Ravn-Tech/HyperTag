# HyperTag

File organization made easy. HyperTag let's humans intuitively express how they think about their files using tags.

## Install
`$ pip install hypertag`

## Quickstart
HyperTag offers a slick CLI but more importantly it creates a directory called ```HyperTagFS``` which is a file system based representation of your files and tags using symbolic links and directories. HyperTag also recognizes a multitude of file types and groups them automatically together into folders (e.g. Images, Documents, Source Code), which can be found in ```HyperTagFS```.

## CLI Functions

### Set HyperTagFS directory path
Default is the user's home directory.

```$ hypertag set_hypertagfs_dir path/to/directory```

### Import existing directory recursively
Import files with tags inferred from existing directory hierarchy

```$ hypertag import path/to/directory```

### Tag file/s
Manually tag files

```$ hypertag tag humans/*.txt with human "Homo Sapiens"```

### Tag a tag
Metatag tag/s to create tag hierarchies

```$ hypertag metatag human with animal```

### Merge tags
Merges all associations (files & tags) of tag_a into tag_b

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

## Inspiration
This project is inspired by other existing open-source projects:
- [TMSU](https://github.com/oniony/TMSU): Written in Go
- [SuperTag](https://github.com/amoffat/supertag): Written in Rust

**What is the point of HyperTag's existence?** HyperTag offers some unique features such as the import function that make it very convenient to use. Also HyperTag's code base is written in Python and thus extremely small (<500 LOC) compared to TMSU (>10,000 LOC) and SuperTag (>25,000 LOC), making it very easy to modify / extend / fix it yourself.

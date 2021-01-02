# HyperTag

File organization made easy. HyperTag let's the user express intuitively how they think about their files using tags.

## Install
Currently there is no easy install available. Gotta clone this repo.

## Quickstart
HyperTag offers a slick CLI but more importantly it creates a directory called ```HyperTagFS``` which is a file system based representation of your files and tags using symbolic links and directories.

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

### Query by tag
Prints all file names associated with tag.

```$ hypertag query human```

### Print all tags

```$ hypertag show```

### Print all files

```$ hypertag show files```

## Architecture
- Python 3.8 powers HyperTag.
- SQLite3 serves as the meta data storage engine.
- Symbolic links are used to create the HyperTagFS directory structure.

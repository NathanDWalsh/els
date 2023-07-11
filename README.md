# eel

eel enables flow of data between multiple sources.

## Dependencies

eel is python based and relies on the pandas library.

```bash
pip install pandas
```

## Basic usage: no yml

When working with data files the command line will work. Pass a path and the contents of the file/folder will be output to the console.

```bash
eel.py test.xlsx       #output all worksheets in test.xlsx to screen
eel.py ./datdir/       #output all data files in ./datdir/ to screen
```

Passing more than one arguments explicitly defines the target (first arg) and the sources (next arguments):

```bash
eel.py ./output/ test.xlsx       #load all test.xlsx worksheets to ./output/*.csv
eel.py ./output/ ./datdir/       #load all ./datdir/ data files to ./output/*.csv
eel.py .janfeb.csv jan.csv feb.csv       #load jan.csv and feb.csv to janfeb.csv
```

## One target : many source convention

Define a target (cntainer or frame) and then one or more sources (cntainer or frame) facilitates:

* Do not Repeat Yourself (DRY) approach for typical ELT environments where a single target database is the typical use case.
* use case where multiple frames feed a single table.

## Advanced usage

When using databases and remote files, the .eel.yml files will have to be used.

## eel object model

eel works with frames and containers as outlined in the below diagram:

```mermaid
---
title: "Abstract ERD: Conatiners and Frames"
---
erDiagram
    CONTAINER ||--O{ CONTAINER: "contains"
    CONTAINER ||--O{ FRAME: "contains"

```

### **Frame:** a tabular data object

* table
* csv
* tsv
* worksheet

### **Container:** a grouping of frames or other containers

* database
* schema
* workbook
* directory
* .eel.yml file
* .eel.yml document

Below are two examples of how containers and frames relate to files and databases:

```mermaid
---
title: "Example ERDs: Conatiners and Frames"
---
erDiagram

    CONTAINER-FOLDER ||--|| CONTAINER-WORKBOOK: "contains"
    CONTAINER-WORKBOOK ||--|{ FRAME-WORKSHEET: "contains"

    CONTAINER-FOLDER ||--|{ FRAME-CSV: "contains"
    
    CONTAINER-DATABASE ||--|{ CONTAINER-SCHEMA: "contains"
    CONTAINER-SCHEMA ||--|{ FRAME-TABLE: "contains"
```

Containers can be

* **homogeneous:** all member frames have the same structure, so they may share the same target frame. (only supported as a source)
* **heterogeneous:** all member frames have different structure, so they should have different target frames.
* **frameless:** no member frames

```mermaid
---
title: Example eel dataflow
---
graph LR
subgraph ./data sources/
    a1[products.csv]
    subgraph transactions.xlsx
        z1[january]
        y1[february]
    end
    subgraph customers.xlsx 
        x1[sold to]
        w1[ship to]
    end
end
subgraph sql db   
    subgraph raw:schema
        a1 --> b1[products]
        z1 --> d1[transactions]
        y1 --> d1
        x1 --> u1[sold to]
        w1 --> v1[ship to]
    end
end
```

In the above dataflow diagram:

* inner boxes are frames
* all other boxes are containers
* transactions.xlsx (left) is a homogeneous container
* sql db (top-right) is a frameless container
* all other containers are heterogeneous

Another way to think about the eel object model is that they form a tree with branches and leafs:

| eel object              | tree analog |
| :---------------------- | :---------- |
| frame                   | leaf        |
| container-homogeneous   | leaf        |
| container-heterogeneous | branch      |
| container-frameless     | branch      |

## Supported containers and frames

The following are supported as both sources and targets.

### Directory

by default, eel considers a folder as a container:

|                    | Type                  | eel object |
| ------------------ | --------------------- | ---------- |
| :white_check_mark: | file folder/directory | Container  |

### Data files

eel recognizes certain file extensions as valid data files:

|                      | Type    | Extension  | eel object |
| -------------------- | ------- | ---------- | ---------- |
| :white_check_mark:   | Excel   | xls[x,b,m] | Container  |
| :white_check_mark:   | text    | csv,tsv    | Frame      |
| :white_large_square: | parquet | parquet    | Frame      |
| :white_large_square: | pickle  | pickle     | Frame      |

### Databases

Database connections can be configured in an .eel.yml file and the following are supported for mvp1:

|                      | Type     | eel object |
| -------------------- | -------- | ---------- |
| :white_check_mark:   | mssql    | Container  |
| :white_check_mark:   | duckdb   | Container  |
| :white_large_square: | mysql    | Container  |
| :white_large_square: | postgres | Container  |
| :white_large_square: | sqllite  | Container  |

## data project: folder/file based

All data sources are defined in the file system, normally contained in a single data source folder.

### Recognized data files are treated as source files and do not require configuration

```bash
yourdata.csv
yourdata.xlsx
```

### Recognized data files can optionally be accompanied by a .eel.yml configuration file

```bash
yourdata.csv
yourdata.csv.eel.yml
yourdata.xlsx
yourdata.xlsx.eel.yml
```

### Other stores such as databases or remote files can be defined in a .eel.yml configuration file

```bash
yourconn.eel.yml
```

### Folders can have a global configuration file which by default propagates to all containing files and subfolders

```bash
.eel.yml
```

## Sources and Targets

An eel flow involves a single data source and target. Sources have some similarities and differences outlines below:



## Sources (the extract bit)

Here are some scenarios for defining a source frame or container:

|           |                                      | file               | dir                | db                 |
| :-------- | :----------------------------------- | :----------------- | :----------------- | :----------------- |
| lone data | data file (.csv, .xlsx)              | :white_check_mark: |                    |                    |
| eel pair  | data file with accompanying .eel.yml | :white_check_mark: |                    |                    |
| lone eel  | .eel.yml with source defined         | :white_check_mark: | :white_check_mark: | :white_check_mark: |

* lone data and eel pair sources can be either a frame or a container
* lone eel sources can be either be a database connection and point to a table or schema or a remote file

## Targets (the load bit)

There are three scenarios for defining a target:


| source type             |    target file     |    target table    |     target dir     |     target db      |
| :---------------------- | :----------------: | :----------------: | :----------------: | :----------------: |
| frame                   | :white_check_mark: | :white_check_mark: |                    |                    |
| homogeneous container   | :white_check_mark: | :white_check_mark: |                    |                    |
| heterogeneous container |                    |                    | :white_check_mark: | :white_check_mark: |

Supported source to target types.

| source type             |    target frame    |  target container  |
| :---------------------- | :----------------: | :----------------: |
| frame                   | :white_check_mark: |                    |
| homogeneous container   | :white_check_mark: |                    |
| heterogeneous container |                    | :white_check_mark: |

* eel.yml
* second argument in the cli call

Defining a target may involve having to define a container and frame separately, here is a typical scenario where multiple source files are loaded to different tables (frames) in a single database (container).

* target database (container) is defined in a glabal .eel.yml file in the source folder
* target frames (tables) are by default the names of the files and/or worksheets
* in the case of a heterogeneous container, the name of the container is the default target frame
* any defaults defined above can be overridden in a global or paired .eel.yml file

An example use case would have a folder containing all source files and a single .eel.yaml file defining a target database container.

```bash
└───data_sources                    # folder
        .eel.yml                    # defines target container for folder
        customers.xlsx              # implicit target frames = names of worksheets
        products.csv                # implicit target frame = file base (products)
        transactions.xlsx           # explicit target frame defined in 
        transactions.xlsx.eel.yml   #  transactions.xlsx.eel.yml
```

### usage

```bash
eel.py ./data sources/  #recursively iterate all folders and files (recognized 
                        #data files + *.eel.yml configs) and load to defined target(s)
```

## config / *.eel.yaml

One or more .eel.yaml files may define:

* frame/container source
  * in file content
  * in file name (eel pair)
* frame/container target
  * in file content
  * inherit from global eel.yaml
* column names and types
* source type: frame or container
* target type: frame or container
* allow parallel=true
* sample n=# or n=%
* environment
* if target exists: drop or truncate or append
* propagete to child folder/files: yes/no
* small-t transformations
* last imported?

## small-t transforms

Some intra-frame transformations are supported:

* add index / unique id
* unpivot / melt data
* include file details
* include frame specific properties from homogeneous containers
* include custom columns defined in eel.yaml

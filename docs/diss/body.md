## Motivation

Declarative data pipelines are not a novel concept [INGESTBASE ref], however when implemented they are normally offered as part of a big data platforms that are integrated in a monolithic system (NiFi, ascend.io) and their realisation occurs in large cloud based infrastructure focusing on big data applications. This proposes the development of a declarative system for ingesting data that focuses on smaller data projects and ease of use.

## Problem Description

```{.mermaid loc=img format=svg theme=neutral caption=dp2}
---
title: Low-level data pipeline with three endpoints.
---
graph LR
direction LR
subgraph "Endpoint Container"
A["Endpoint Table:\nData Source"]
end
subgraph "Endpoint Container"
A -->|Extract| B[Endpoint Table:\nData Transit]

end
B -. Transform .-> B
subgraph "Endpoint Container"
B -->|Load| C[Endpoint Table:\nData Target]
end
C -. Transform .-> C

```

The early stages of a data project often involve [insert refs CRISP-DM, DataONE, Data Engineering] a consolidation of multiple datasets into a single datastore or project spanning one or more tables. This paper will use the data engineering term _data ingestion_ to refer to this phase, concretely: the process of copying data from one or more sources to a single target. [Figure @fig:dataflow] below shows and example of a dataflow with multiple files landing in a single database scema, although the destination coud also be a python project or another set of files in a folder.

```{.mermaid loc=img format=svg theme=neutral caption=dataflow}
---
title: Example dataflow
---
graph LR
subgraph \nFILE SYSTEM
  fs[ ]
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
end
subgraph \nDATABASE
    db[ ]
    subgraph raw:schema
        a1 --> b1[products]
        z1 --> d1[transactions]
        y1 --> d1
        x1 --> u1[sold to]
        w1 --> v1[ship to]
    end
end
style fs height:0px
style db height:0px
```

Setting up the data ingestion process often involves many manual steps of data discovery, cleansing, standardisation and mapping to bring it into the target datastore. Depending on the project type the logic for these transformations can take various forms, from manually copy pasting data into an Excel spreadsheet to coding transformations in a python/pandas project. This makes communicating the steps taken to in this step a challange. It can also introduce reproducability issues when a decision to change a data definition, source or target while keeping the remainder of the logic consistent.

To summarise, the ingestion phase of a data project presents some challanges:

1. Manual logic defined that could otherwise be automated (mapping of fields, definition of datatypes)
2. System dependant: work done is not easily trasnferred to an alternative system (change in database backend, switching from in-memory analysis to datastore)
3. Lineage: steps taken to transfer data not easily descernable from differing logic by missing manual steps, decoding logic used in different systems

## Proposal

The project proposed below A platform agnostic configuration system/language that can describe data ingestion can alleviate some of the problems outlined above.

1. Minimal configuration: data introspection for detecting tables, fields and data types.
2. System agnostic: source and target datastores can easily be changed.
3. Lineage can be read directly from configration.

What is missing

When available as standalone solutions (meltano), they involve a complicated setup process and are built upon a deadware technology (singer). It could also be argued that the methods for extracting and loading of data are imperative, where the configuration must explictly state the connectors for extraction and loading of data.

This paper proposes a minimal-configuration solution for data ingestion: extracting from and loading to common data formats. that will perform introspection on data sources and destinations in order to

This paper proposes a project that will develop and implement a first phase of a project that will eventually be the first phase of a system for creating declarative pipelines. Focusing on the ingestion phase and common formats, the system could be extended to account for transformation phase.

## Project

This project has three main modules that will be developed in concert:
eel-project, a folder-based project specification with eel-yaml, a
pipeline configuration language, and eel-cli, a command line tool
which interprets and executes eel-project. [Figure @fig:sequence]
below is a high-level sequence diagram as to how these modules
interact with a data pipeline.

```{.mermaid loc=img format=svg theme=neutral caption=sequence}
---
title: Sequence of eel interactions across modules and data stores
---
sequenceDiagram
    participant y as eel-project<br>eel-yaml
    participant c as eel-cli
    participant s as data stores:<br>sources, trargets
    c->>y: read explicit config
    c->>s: validate explicit config
    c->>s: infer implicit config
    opt
      c->>y: write inferred config
    end
    opt
      c->>s: execute pipeline
    end
```

## Principal Goals

This scope of this project will focus on the usability aspect of the
language and tool:

- Design a declarative language (YAML) and accompanying project
  structure for extracting and loading data to/from standard file
  formats or databases, including in-process memory structures as
  targets (i.e., pandas data frames).

- Design CLI system to interpret language and optimally orchestrate
  extract/loading procedures, allowing for configuration trees (for
  inheriting config from parent branches) and inferred configuration through data introspection
  (i.e., type inference, reading existing meta-data from sources).

- Allow non-contextual transformations to be defined (i.e. column
  which defines source, index column)

## Secondary Goals

These will not be built into the initial project, but design
considerations will be taken in order to allow for the implementation of
the following features.

- Column-level data providence should be built into the system.

- Ability to optimize extract-load procedures based on available tools
  in the executing system, for example utilizing database-native
  extract-load procedures when using a particular database as a
  source/target.

- Multi-thread and/or multi-process when speed/performance gains
  likely.

- Contextual transformation support.

## Background

a short description of how previous work addresses (or fails to address)
this problem.

## Methods

A description of the methods and techniques to be used, indicating that
alternatives have been considered and ruled out on sound scientific or
engineering grounds.

## Evaluation

Details of the metrics or other methods by which the outcomes will be
evaluated.

## legal, social, ethical or professional issues

## Workplan

A timetable detailing what will be done to complete the proposed
project, and when these tasks will be completed.

### eel-yaml

A human-readable declarative configuration language defined in a YAML
schema that can be used in most popular editors (such as Visual Studio
Code) to facilitate the user's ability to create the extract-load
configurations manually. The declarative language can be considered a
configuration which details data targets and sources.

### eel-project

A folder/directory representing the top level of an eel project, all
subfolders and files are considered part of the project's contents. All
folders and certain file types are handled as follows:

- Subfolders are used to organize a hierarchy of targets and/or
  sources in the pipeline, a special configuration file \_.eel.yml is
  used to set the configuration that will be inherited by all
  containing files and folders.

- Recognized data files such as .csv and .xlsx will be treated as
  implicit source configurations, so a .csv file will be treated as
  first a configuration file which refers to itself as the source
  file.

- Complimentary eel.yml files can augment implicit source
  configurations with additional information such as encoding of the
  csv. In for a complimentary file to be recognized as such, it should
  have the exact same name as the recognized data file with the
  additional .eel.yml extension. For example: data1.csv is recognized
  as an implicit source while data1.csv.eel.yml explicitly defines the
  encoding that should be used.

- Sole eel.yml files (those which are not paired to a data file) may
  contain data source and/or target configurations.

The project will stress DRY principals allow for minimum configurations
using some of the following methods:

- Configuration inheritance via directory/folder structure:
  configurations inherit from parent node (i.e., folder). Useful when
  a target object (i.e. database or table) is the same for multiple
  data sources.

- Use existing metadata (i.e., from database schema) for data types.

- Type inferencing when no data type information available, for
  example in csv files.

Configuration and language are two terms that are used interchangeably
in this document, and both refer broadly to the eel-yaml and eel-project
modules.

### eel-cli

A minimum viable product for a command line tool that will interpret
eel-project and perform certain actions:

- Show a preview of how eel-cli interprets the current eel-yaml
  project:

- config tree

- task flow tree

- eel-yaml inherits

- execute dataflow

execute the extract-load procedures and also provide granular
information on the extractions.

## Roadmap

Although these three parts of the project are considered complimentary
and will be developed in concert, it is expected that they would
eventually split and evolve into separate projects in order to separate
the declarative language from any future interpreters that could be
developed to support it.

## Examples for Citations

Here are examples for citations [@P2] and [@P2].

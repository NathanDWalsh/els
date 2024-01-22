## Purpose

Declarative data pipelines are not a novel concept, however they are offered as part of big data offerings that are often integrated in a monolithic system (NiFi, ascend.io) their realisation occurs in large cloud based infrastructure focusing on big data applications. Herein is proposed is the first phase of a system for creating declarative pipelines. Focusing on the ingestion phase and common formats, the system could be extended to account for transformation phase.

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
  inheriting config from parent branches) and inferred configuration
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

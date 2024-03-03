## Introduction

> Data pipelines are sets of processes that move and transform data from various sources to a destination where new value can be derived. [@pipelines_pocket, p. 1]

Data pipelines are an important part of today's data landscape, underpinning a vast array of processes: from manually created ad-hoc reports in the form of spreadsheets to advanced LLMs powering ChatGPT. Their purpose is to make data available and accessible to data consumers.

A basic unit of a pipeline moves data from a source to a target while optionally tranforming the data in the process as iullistrated in [Figure @fig:sequence0]. _Transformations_ change the state of the data: some basic examples include cleaning, removing and summarising data.

```{.mermaid loc=img format=svg theme=neutral caption=sequence0}
---
title: Basic data pipeline sequence.
---
sequenceDiagram
    participant y as Data Source
    participant c as Pipeline Engine
    participant s as Data Target
    c->>y: Pull data
    opt
      c-->c: Transform data
    end
    c->>s: Push data
```

For the last ten years I have been working as a data integration specialist, creating and running several data pipeline projects simultaneously. This motivated me to think about how data pipeline projects can be rapidly implemented, iterated and change traced. The challanges that this paper addresses are those that occur in the beginning of data pipelines: pulling data from multiple disparate sources and pushing them to a single source.

### Problem Statement

Source data for pipelines are normally provided in a varity of formats and layouts. Customer provides data in a variety of sources that needs to be loaded to a database for further processing. Data sources are frequently updated, sometimes sources are added, removed and replaced. A solution is required that has the ability to update the target database reflecting the changes to the source. Data sources can change from file-based to connection/API-based as the project matures. For example, a customer table once provided as a csv file could change to an API connection which can directly query the customer data.

Setup conditions:

1. Any files containing source data are not permitted to be manually modified.
2. Any changes to source data should occur in the transformation step by the pipeline engine.
3. Transformations should be minimal, unless required to make the data fit into a table at the target.

The alternative involves creating several disparate scripts, each customised for a particular data source and its settings.

A typical data pipeline has several sources in distinct formats. This creates a necessesity to tailor an import script to account for each system in a different way.

The use case being addressed in this paper is taking data from multiple sources and importing it into a database.

This use case describes a data scientist connecting to or downloading one or more data sources for consolidation and analysis in Python. This process can be manual, automated or a combination of both.

Flow:

1. Data Scientist downloads data in Excel, csv or similar file formats from online portal or application, and/or connects directly to data via database connection or API.
2. Data Scientist creates script(s) to load datasets into pandas dataframes.
3. Data Scientist scripts transformations on data.
4. After running scripts, data is ready for analysis in pandas dataframe endpoints.

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

Below is a typical high-level outline for the technical aspects of a data pipeline project I would follow:

<!-- I developed a workflow for working with new data projects as follows: -->

1. Create a SQL database for staging.
2. Import all source files into the database as tables.
3. Create views/tables to perform transformations on the data.
4. Export transformed data to target endpoint for downstream consumption.
5. Implement changes to sources, transformations as requirements change and/or sources modified and repeat steps 1-4.

```{.mermaid loc=img format=svg theme=neutral caption=dp1}
---
title: Data pipeline project phase 1 as-is.
---
graph LR
  T["Web"] -->|"Download"| B[File\nSystem]
  T -->|Scrape Script| E
  F["Email"] -->|"Download"| B[File\nSystem]
  B -->|"Load\nScript"| E
  S["API"] -->|Load\nScript| E[Python]
  E -->|Transform\nScript| E
  E -->|Load\nScript| Q[SQL DB]
  Q -->|Transform\nViews| Q
  Q -->|Load\nScript| X[Target]
```

Managing change was one challange that is partially addressed in the transformation layer. The idea is to keep everything as text so that transformations can be easily traced in order to be able to explain what transformations took place on the data simply by looking at, or deriving from the logic present in the SQL endpoint which performs the transformations.

The part that was more difficult to trace is the importation of source data into the transform endpoint (SQL database). For any given project source data could come in a myriad of formats and/or endpoints. To name a few, it would be quite common to receive data in Excel, csv and fixed width files, APIs as well as html downloads. Each of these files could have different layouts and structure as well.

<!-- 5. Communicate to business users all data sources and transformatoins that were performed to create the target data, leaving out technical deatils.
5. Communicate the same to the technical team, but with more technical details. -->
<!-- In addition to the above actions, the following tasks were also required: -->

<!-- , depending on the service level. For example a premium SLA would provide all of the logic performed on the intermediate tables while a basic SLA would provide only an explaination of the logic between source and targets. -->

<!-- In summary, my use case is divided into two related categories: create and maintain a data pipeline (steps 1-4) and explain the data pipeline steps (5-6). -->

<!-- #### Data pipeline creation and maintenance -->

When a data project begins, it is helpful to have a quick way to get it started with the source data available. The source data is often supplied in the form of files delivered by a combination of business and technical support staff. The solution I settled on is to save all source files in a folder and having a python script import each file to the database. This script would infer the name of the target table based on the file's name or contents, for example:

- if it is an Excel file, import each worksheet into it's own table using the name of the worksheet as the table name.
- if it is a csv file, import into a table using the name of the file as table name.

This worked for many cases, but there were cases where this default behaviour would not be sufficient. For example, in some cases the sheets in the Excel file should all go to the same table and the sheet name should be a column in that table. In this case I would create ad-hoc workarounds in the code or put hints in the filename or sheet name that would trigger a different action in the import script. In other cases I would manually modify the source files to allow for the defaults to be acceptable. One workaround I commonly used is to put a "TRUNCATE" keyword in the filename so the script would know to first remove all existing rows in the database before importing.

<!-- This solution also presented additional challanges:

- after a file is imported into the database, it was moved to an "imported" folder. If this file was later descoped the original file would have to be manually deleted.
- if a file is updated with more information, the appropriate logic would have to be triggered to update/replace the table in the database if it has a new structure. One workaround I commonly used is to put a "TRUNCATE" keyword in the filename so the script would know to first remove all existing rows in the database before importing. -->

This was the first inspiration for what is being proposed below: a declarative configuration language for the import phase of a data pipeline. Instead of relying on ad-hoc changes to scripts and adding keywords to the files, a configuration language should exist to cover all of the common tasks and exceptions and rules that should be applied to the importation of data. If no configuration accompanies a file, then defaults will be applied to the import.

<!-- #### Data pipeline explanation

As a data project progresses, communicating how data has been moved and transformed from the sources to target. This is important on two fronts and each have their own requirements:

- Communicate pipeline to the business to ensure it aligns with business requirements.
- Communicate to technical team in order to ensure a smooth handover for sustainable phase of the project.

I would aproach this part of the requirement by using scripts to inspect the SQL that defined the transformations and create a lineage graph which would demonstrate how source data is mapped to the target data. This was cumbersome and error-prone as the same SQL can be expressed in many ways.  -->

As outlined in the use cases above, there are different methods and tools used for defining a pipeline. Some are manual, such as downloading a dataset and importing into Excel, whiles others are automated using an ETL. A problem arises when users across these different use cases are working together.

The initial inspiriation for this project was born out of a having an easy way to use multiple files stored in a single directory as a source for a data pipelie.

- there lacks a standard way to quickly move multiple sources into a single destination, allowin for sensible defaults but allowing changes easily.
-

- Section 1.1 brings some challenges. I would restructure the intro as follows:

  - present challenges that users are facing; perhaps no need for both Figures 1 and 2; I like them but one figure may be sufficient to convey the same messages

  - Why are these challenges difficult and cannot be solved using existing tools/techniques?

  - expand the explanation of the use case from that figure (who does what, individual steps); after reading the two paragraphs from Sec 1.1, not sure that the problems are clearly explained

- having an example of configuration would be helpful to the reader

- _lineage_: In the context of data pipelines, metadata which explains how data has been moved and/or transformed from source to target.

- Although technologies for managing pipelines are always changing, the underlying fundamentals of the data itself remains the same. A table is always a table, a row is always a row. However when tooling changes it often necesetates a rework of defining the data in the new system.
- Different users use different tools and processes for accessing the same data. Sometimes a pipeline may be passed from one team to another, and redundancy is introduced as each new team has to reproduce the pipeline in the tools of their domain.
<!-- - Data pipeline explanability is not straightforward, oftern requiring manual creatiion of directed graphs to explain how the data pipeline has been executed. Adding further complication, sometimes graphs of differing details are required for different audiences (technical vs. non-technical).
- As defined at the top of this chapter, data pipelines normally move data from one or more sources to a single destination. Much of the tooling available today does not necessarily take the many to one paradimn into account, requiring a user to redefine the destination for each source, introducing further redundancy. -->
- With an ever increasing amount of data end points available for analysis, it should be trivial to change destinations of pipelines to test for speed/cost optimisations. However, existing tooling makes swapping an end point for a data pipeline cumbersome and error prone.

<!-- As a permanent member of staff creating and maintaining, data pipelines were a relatively minor part of my work-load. However as a consultant working on many projects mostly in their inception, I have come across many anti-patterns that can be improved upon with a new solution proposed below. -->

Each programming language, database platform, ETL system and data analysis software has distinct methods for implementing extraction and loading (ingestion) logic. Lacking is a system that can be used interchangeably across different formats and tools. A user-friendly system for declaring data ingestion can have several use cases, some of which are as follows: (1) less time spent writing code to extract, load, move or convert data; (2) easily swap a pipeline from one datastore to another; (3) Seamlessly migrate data projects from in-process to physical storage; and (4) generate data lineage directly from the configuration.

<!-- Listed in [Table @tbl:pipelines] is a range of use cases for the data pipeline, each with its own set of tools and methods for ingesting the required data, this point is further illustrated in [Figure @fig:sequencedp]. -->

Lacking is a clear way to define and communicate data ingestion across different use cases and end points. [Figure @fig:sequencedp] shows how a data pipeline may pass between different members of a team, each with their own tooling and methods for handling the data. This introduces process redundancy as each participant is re-creating, or converting a pipeline into their own tooling in order to ingest the same data. Some of the ingestion work performed in point 1 is repeated in points 3, 6 and 9.

<!-- Each user has their own set of tools and processes for ingesting the data into the data product. -->

```{.mermaid loc=img format=svg theme=neutral caption=sequencedp}
---
title: Evolution of an internal-facing data product.
---
sequenceDiagram
    autonumber
    box
      actor Business as Business User
      actor Scientist as Data Scientist
      actor Engineer as Data Engineer
      actor DBA as DBA/Cloud Engineer
    end
    participant Data
    Business->Data: Manually ingest: download and import into Excel
    Business->>+Scientist: Can you enhance<br>what I did in Excel
    Scientist->Data: Write a Python script to ingest
    Scientist->>-Business: Sure, run this<br>script to refresh
    Business->>+Engineer: Can you automate this?
    Engineer->Data: Use ETL tools to ingest on schedule
    Engineer->>-Business: Sure, here it is in real-time
    Business->>+DBA: Can we save money on this?
    DBA->Data: Test ingestion with<br>different end points
    DBA->>-Business: Sure, here it is in a cheaper backend
```

Point 9 in [Figure @fig:sequencedp] also highlights another problem that can arise with data pipelines: the ability to change end points seamlessly. In this example the DBA or cloud engineer may be experimenting with different database backends to find the most effective solution, optimising for cost in this case or speed in another. Allowing a mechanism to easily test and change end points can reduce vendor lock-in risk when building data products.

<!-- , leaving free the option to test and move to different end points/vendors. -->

<!-- Each subsequent actor is building on work done previously, but using different tooling to accomplish a similar goal. -->

<!-- DAGs are also used in data engineering to build task-based workflows, this solution will use DAGs to illustrate data lineage. -->

 <!-- This addresses the issues of table-level lineage and column-level lineage: the former providing a high-level detail of a dataflow and the latter providing a lower-level detail. Although lineage visibility is available in some tools used in analytics and ETL packages, there is lacking a standard that can be used and understood by all. -->

<!-- For example, a data scientist may begin working with data to create a data product and when completed hand it over to a data engineer for automating. In this example, the data engineer may be using python scripts to import source data into dataframes using Python scripts, while the data engineer may use ETL tooling to define a pipeline which stores data on disk for later retrieval by the data product. This highlights one of the challenges with managing data pipelines: they are often built with varying toolsets that include one or more languages and/or toolsets. -->

<!-- Setting up the data ingestion process often involves many manual steps of data discovery, cleansing, standardisation and mapping to bring it into the target data store. Depending on the project type the logic for these transformations can take various forms, from manually copy pasting data into an Excel spreadsheet to coding transformations in a python/pandas project. This makes communicating and converting these transformations across different teams a challenge. -->

<!-- It can also introduce reproducibility issues when a decision to change a data definition, source or target while keeping the remainder of the logic consistent. -->

<!-- To summarise, the ingestion phase of a data project presents some challenges:

1. Redundancy when transferring to different tools/databases/platforms.
2. System dependant: work done is not easily transferred to an alternative system (change in database backend, switching from in-memory analysis to data store)
3. Lineage: steps taken to transfer data not easily discernable from differing logic by missing manual steps, decoding logic used in different systems -->
<!-- 4. Manual logic defined that could otherwise be automated (mapping of fields, definition of data types) -->

<!-- To summarise, there is lacking a general language for describing a data pipeline and its transformations that can be used to change end points . -->

<!-- This makes it difficult to have a common cross-system language for describing such pipelines to facilitate the swapping of one end point for another. -->

### Proposed Solution

> The great virtue of a declarative language is that it makes the intent clear. You're not saying how to do something, you're saying what you want to achieve. [@patterns_eaa, p. 39]

The solution proposed below seeks to create a declarative configuration system for data ingestion that can be used across different use cases. The name of this solution is _eel_: easy-extract-load and some of the project's goals are listed in [Table @tbl:goals].

<!-- One benefit of such a system is that it can facilitate communication between an interdisciplinary team with a common language to describe how data is ingested and transformed. -->

: Eel project goals. {#tbl:goals}

| Goal        | Measure                                             |
| ----------- | --------------------------------------------------- |
| 1. Friendly | Configuration optional, inferring suitable defaults |
| 2. Readable | Text-based, Human-readable configuration language   |
| 3. Agnostic | Works across different end points seamlessly        |

<!-- | 4. Explainable | Lineage graphs included                             | -->

More details on the project goals from [Table @tbl:goals]:

1. Friendly and intuitive: should be easy for a user to get started without the burden of setting up configuration files manually.
2. Readable plain text configuration: All configuration in logical folder structure and with one or more plain text configuration files defining end points and transformations.
3. Technology agnostic: should be able to swap different end-points without requiring a re-write of mapping/transformation logic.
4. Explainable: system can create lineage graphs explaining the mappings and transformations defined in the configuration.

- Table 4 seems redundant, just integrate it into text

Revisiting [Figure @fig:sequencedp] with eel, points 3, 6 and 9 could be using the same eel configuration which defines the ingestion instead of redefining the ingestion across different tools. Likewise, [Figure @fig:sequencedp2] can be remained by creating the lineage graphs in step 5 automatically and reducing the need for communication between the Data Scientist and Data Engineer (points 8-10), since all required ingestion information is already included in the eel configuration.

Points 2 and 11 in [Figure @fig:sequencedp2] highlight an additional benefit of the solution: the ability to switch from an in-memory storage solution to a persisted solution. This can be especially helpful for rapid prototyping and testing of data products.

<!-- - Design a declarative language (YAML) and accompanying project
  structure for extracting and loading data to/from standard file
  formats or databases, including in-process memory structures as
  targets (i.e., pandas data frames).

- Design CLI system to interpret language and optimally orchestrate
  extract/loading procedures, allowing for configuration trees (for
  inheriting config from parent branches) and inferred configuration through data introspection
  (i.e., type inference, reading existing meta-data from sources).

- Allow non-contextual transformations to be defined (i.e. column
  which defines source, index column) -->

<!-- Logical project/configuration structure -->
<!-- A project definition is flexible:
- A single data file such as a csv.
- A single yaml file defining end points.
- A directory containing one or more of the above, with an arbitrary number of sub-directories.
- Defaults are generated by the system and can optionally be written as configs to be modified. -->
<!-- - Minimal configuration: data introspection for detecting tables, fields and data types. -->

<!-- With a common way to share ingestion pipelines, they will also have access to a common way to communicate data lineage.

Additional objectives for the project are as follows: -->

<!-- The scope of this solution is a subset of a data pipeline ([Table @tbl:elt]) and a subset of transformations ([Table @tbl:tfm]) in order to develop a system that may be built upon in later phases. -->

<!-- This paper proposes a project that will develop and implement a first phase of a project that will eventually be the first phase of a system for creating declarative pipelines. Focusing on the ingestion phase and common formats, the system could be extended to account for more transformations in a later project phase. -->

<!-- To fully develop a data pipeline language is a large undertaking. Doing some quick calculations, let's say there are 10 file formats + 10 databases leaving us with 20 different possible data stores. Just "converting" each of these stores to another leaves us with 400 simple transformations to support and test. This leaves out that transformations vary and some are so custom that to define them in simple terms may not be possible. Let's say that 20 different simple transformations can be defined, this leaves us with 8000 different possibilities to test.

Given the scope of such an undertaking, the below proposal will narrow the scope to the early part of the pipeline known as _data ingestion_. Data ingestion is the part of the pipeline which moves data from one data store to another data store, optionally applying non-contextual transformations. -->

<!-- What will be proposed below is the creating of a common configuration language for defining a data pipeline that can be shared across functions and that can work with different data formats, databases or data stores. [Table @tbl:scope] summarized. -->

Modern data pipelines have many features: most of which will not be in scope for this project. [Table @tbl:scope] lists elements of a modern data pipeline and how they are scoped in relation to this project.

: Project scope summary. {#tbl:scope}

| Element          | In Scope       | Out of Scope | More Details     |
| ---------------- | -------------- | ------------ | ---------------- |
| Sub-pattern      | Ingestion/EtL  | ETL/ELT/EtLT | [Table @tbl:elt] |
| Transformations  | Non-contextual | Contextual   | [Table @tbl:tfm] |
| End points       | See Details    | All others   | [Table @tbl:ep]  |
| Schema type      | Tabular        | Document     |                  |
| Concurrency      | Single-thread  | Multi-thread |                  |
| Scaling          | Single-node    | Multi-node   |                  |
| Locality         | Local/Network  | Cloud/Online |                  |
| Transform Engine | python/pandas  | Dynamic      |                  |
| Load table       | Insert/Create  | CDC/Continue |                  |
| Lineage support  | Table-level    | Column-level | Data-based DAGs  |
| Method           | Batch          | Streaming    |                  |
| Interface        | CLI/Yml schema | GUI          | Use IDE/editor   |
| Orchestration    | Static         | Dynamic      |                  |
| Tracking/Stats   | Not in Scope   |              |                  |
| Security         | Not in Scope   |              |                  |

- Table 5 -- lots of unexplained technical terms, not very useful to the reader (remember your thesis will be read by non-experts) in understanding what you're trying to achieve

[Table @tbl:scope] lists some of the end points that will be supported in this phase of the project, those not listed are implicitly not in scope. Note the distinction between container and table: for the purposes of this project a container is analogous to a directory which contains one or more tables and/or containers.

: Project scope data store end points. {#tbl:ep}

| Class      | Store     | Capacity  | Scope: source | Scope: target |
| ---------- | --------- | --------- | ------------- | ------------- |
| File       | Excel     | Container | Yes           | Yes           |
| File       | csv       | Table     | Yes           | Yes           |
| Database   | mssql     | Container | Yes           | Yes           |
| Database   | sqllite   | Container | Yes           | Yes           |
| In-process | dataframe | Table     | No            | Yes           |

<!-- To fully develop a mature data pipeline language is a large undertaking. Doing some quick calculations, let's say there are 10 file formats + 10 databases leaving us with 20 different possible data stores. Just "converting" each of these stores to another leaves us with 400 simple transformations to support and test. This leaves out that transformations vary and some are so custom that to define them in simple terms may not be possible. Let's say that 20 different simple transformations can be defined, this leaves us with 8000 different possibilities to test. -->

<!-- Given the scope of such an undertaking, the below proposal will narrow the scope to the early part of the pipeline known as _data ingestion_. Data ingestion is the part of the pipeline which moves data from one data store to another data store, optionally applying non-contextual transformations. -->

<!-- | DAGs            | Data-based     | Task-based   |                  | -->
<!-- | Versioning      | Implicit       | Explicit     | Text-based       | -->

### Timeliness and Novelty

<!-- With the increased focus on AI and LLMs, a way to explain and communicate data is ever important. The proposed project is a re-evaluation of data transformation using an intuitive project structure. Starting with a solid foundation in the early stages of data extraction, it seeks to lay a common groundwork for a general data transformation syntax that may be extended for use in more transformations. -->

<!-- In the current context of AI and LLM hype a la ChatGPT, this project is a back to basics re-think of how data pipelines can be generalized and communicated across differing platforms. With a good benchmark, AI tools can be further used to optimize some of the data transformation processes defined herein. -->

Although the theory of a declarative ETL has been discussed in academia [@ingestbase] and industry [@mded], there lacks a general purpose implementation. This project seeks a back to basics data approach, starting with small, single node data projects. If successful at the small data scale, it could serve as a foundation for a more advanced system for defining pipelines that have a greater scope.

<!-- The proposal herein seeks to get such a system started by beginning with a basic framework beginning with some common data formats and databases. -->

<!-- Much attention is paid to big data projects that feed into LLMs and AI models.  -->

### Feasibility

A fully functional data pipeline system built from scratch is a large undertaking. To keep the scope realistic given limited time and resources, care has been taken to keep the scope of this project small. See [Table @tbl:scope] for details on scope.

<!-- This  in order to create a good proof of concept that may later be built on. -->

<!-- Given the amount of data end points in use and adding to that the types of transformations. The first phase of this project must be minimally scoped to demonstrate its usefulness as a concept. See tables, x,y,z for details on scope.

In order to ensure that the project is feasible, a working prototype has already been completed that fulfills requirements x,y,z. -->

### Beneficiaries

<!-- This is more a practical project than a research project. However the tooling itself could be a benefit to researches if adopted as a means of defining the data used as part of the research process. -->

The main beneficiaries of this project should be the following:

- Data engineers who work on small to medium data pipelines can use this system to define their data ingestion.
- Data scientists who want a common language to define (and share) their data sources outside of the code-base.
- Data professionals who wish to migrate data from one end point to another.

<!-- Since data scientists are by far the most numerous of the potential beneficiaries for this project, a decision has been made to build the tool in python with popular data science package pandas. Also supported is panda dataframes as end points which could further increase adoption. -->

## Background and Related Work

Declarative data pipelines are not a novel concept [@ingestbase], however when implemented they are normally offered as part of a big data platforms that are integrated in a monolithic system [@nifi; @ascend] and their realisation occurs in large cloud based infrastructure focusing on big data applications. When available as standalone solutions [@meltano], they involve a complicated setup process and are built upon a technology [@singer] that is no longer under active development.

<!-- This paper proposes the development of a declarative system for ingesting data that focuses on smaller data projects and ease of use. -->

<!-- It could also be argued that the methods for extracting and loading of data are imperative, where the configuration must explicitly state the connectors for extraction and loading of data. -->

## Programme and Methods

The project will be planned using the waterfall method but executed more flexibly as required. This allows for clear planning pathway while being open to changes as the project progresses.

This project has three main modules that will be developed: (1) eel-yaml, a pipeline configuration language, (2) eel-project, a folder-based project specification, and (3) eel-cli, a command line interface
which interprets and among other functions, executes the pipelines defined in an eel-project. [Figure @fig:sequence] below is a high-level sequence diagram as to how these modules interact with a data pipeline.

```{.mermaid loc=img format=svg theme=neutral caption=sequence}
---
title: Sequence of eel interactions across modules and end points.
---
sequenceDiagram
    participant y as eel-project<br>eel-yaml
    participant c as eel-cli
    participant s as end points:<br>sources, targets
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

### Work packages

The following four work packages are planned to be developed in sequence with some overlap in between. Each work package has approximately 2 weeks of development, 1 week of waiting for supervisor feedback and a final week of revision. With the exception of _Project setup_, each work package focuses on a single eel module.

<!-- Although the three modules of the project are considered complimentary
and will be developed together, it is expected that they might
eventually split and evolve into separate projects in order to separate
the declarative language and project from any future interpreters that could be developed to support it. -->

#### Project setup

<!-- Note in [Table @tbl:scope] the entry for Transform Engine has python/pandas listed in scope. A truly declarative approach could dynamically choose a transform engine depending on resources available and/or end points. -->

This work package will develop a testing and benchmarking suite that will be used as development progresses in the project

Work package tasks:

1. Determine datasets to be used in tests.
2. Develop a battery of tests to be performed in concert with each work package.

#### eel-yaml: configuration language

A human-readable declarative configuration language defined in a YAML
schema that can be used in most popular editors (such as Visual Studio
Code) to facilitate the user's ability to create the extract-load
configurations manually. The declarative language can be considered a
configuration which details data end points (targets and sources.)

Work package tasks:

1. Create and document yaml schema in concert with a python interpreter.
2. Test interpreter/yaml against tests developed in project setup.

#### eel-project: intuitive project structure

A folder/directory representing the top level of an eel project, all
sub-directories and files are considered part of the project's contents.

<!-- All
folders and certain file types are handled as follows:

- Sub-folders are used to organize a hierarchy of targets and/or
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
  contain data source and/or target configurations. -->

The project structure will stress DRY principals allow for minimum configurations
using some of the following methods:

- Configuration inheritance directory/folder structure:
  configurations inherit from parent node (i.e., folder). Useful when
  a target object (i.e. database or table) is the same for multiple
  data sources.

- Use existing metadata (i.e., from database schema) for data types, table and container names.

- Type inferencing when no data type information available, for
  example in csv files.

<!-- Configuration and language are two terms that are used interchangeably
in this document, and both refer broadly to the eel-yaml and eel-project
modules. -->

Work package tasks:

1. Yaml interpreter to account for a project hierarchy, including passing of configuration to child nodes.
2. Yaml interpreter to output defaults to yaml project directory.

#### eel-cli: interface development

A basic a command line tool that will interpret
eel-project and perform certain actions:

- Show a preview of how eel-cli interprets the current eel-yaml
  project:
  - config tree
  - task flow tree
  - eel-yaml inherits
- Execute dataflow
<!-- - Execute the extract-load procedures and also provide granular  information on the extractions. -->

Work package tasks:

1. Fine-tune interpreter to work with a cli, beginning with pipeline execution.
2. Add preview functionality to the cli to allow the user to see how a pipeline will be executed before committing.

#### Dissertation

The dissertation will be written throughout the entirety of the project. With the end of each work unit, the dissertation will be submitted to the project supervisor for review. Feedback on dissertation work units are expected after a week of submission as reflected in the Gantt chart in [Figure @fig:gantt].

#### Progress Report

Not strictly a work package, but a mid-point progress report to be developed containing a summary of work done and remaining tasks.

### Risk Assessment

This project has some risks associated with it listed in [Table @tbl:risks].

: Risk assessment for project. {#tbl:risks}

|     | Risk Description                       | Impact | Likelihood |
| --- | -------------------------------------- | ------ | ---------- |
| 1   | Misjudgments in scheduling             | High   | Medium     |
| 2   | Existing solution solves problem       | Low    | Low        |
| 3   | AI Emergence renders project redundant | Medium | Medium     |

The following mitigating actions have been taken to reduce the risks listed in [Table @tbl:risks]:

<!-- a prototype of the system has already been developed. -->

1. To reduce the likelihood of a misjudgment in scheduling, the project is minimally scoped.
2. Extensive research has been conducted to find existing solutions to the problems posed in this document.
3. Popularity of AI/LLMs and prompt engineering for data analysis is nascent but progressing fast. However it is expected that there will still be a need for communicating data pipelines with humans. As AI/LLMs matures, it could be used to rapidly expand the project's scope in later phases
<!-- and the use of LLMs could present an opportunity to supercharge this project after initial phase has been completed. -->

### Ethics

This project has no no ethical concerns as defined in [@ethics].

## Evaluation

Evaluation will be based on the results of tests developed in the project setup phase. An example of a basic test is presented in [Figure @fig:roundtrip]. This round-trip test moves a dataset between different end points, landing in the same end point format in which it started. A simple hash test could be performed to test if anything has been modified in the process.

```{.mermaid loc=img format=svg theme=neutral caption=roundtrip}
---
title: Round-trip test
---
flowchart LR
  subgraph Round Trip
    csv1[csv:source] --> postgres --> excel  --> csv2[csv:target]

  end
  subgraph Validate
    csv1 --> hash
    csv2 --> hash
  end
```

This is a very basic test that does not cover transformations: only the extract and load parts. There are other problems with this type of test as follows:

- Order of records and encoding need to be preserved for test to pass.
- Transformations not tested.
- Not clear how are empty strings, nulls and zeros are handled.
- Not clear how data types are handled.

This is just one test of many that should be created in the project setup phase and agreed with the supervisor.

<!-- Other tests should be created to address some of the above limitations, performed in phase 1. -->

<!-- Since the principal goal of this project is usability, this part of the evaluation is difficult to perform without a separate data collection project surveying user's reactions to it.

With the absence fo the ability ot test usability aspects, the evaluation will be done on the technical aspects of the functionalities.

- test how long it takes to perform manually
- compare with how long it takes to do in the new method
- setup time vs running time
- test if ingestion occurs correctly without transformation (round-trip test)
- test a subset of transformations to ensure they are performed correctly

1. User
2. System able to define sources and targets with minimal configuration
3. Ability to change sources and target data stores, including in-process.
4. Generate

reference a table of objectives in another table?

Work phase 1 will define some measurable objectives in the form of benchmark/tests etc. With the datasets defined in this phase, the project will be considered a success if:

All datasets can do a round-trip test: moving to each of the different formats and then back to the original format (csv), where a binary check can be done on the final file format to ensure no data is lost. -->

## Expected Outcomes

The expected outcome is a python package that fulfills of the goals listed in [Table @tbl:goals], as well as an accompanying dissertation containing a description of the work undertaken and an evaluation of the resulting system.

<!-- ### Principal Goals

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
  which defines source, index column) -->

<!-- ### Secondary Goals

These will not be built into the initial project, but design
considerations will be taken in order to allow for the implementation of
the following features.

- Column-level data lineage should be built into the system.

- Ability to optimize extract-load procedures based on available tools
  in the executing system, for example utilizing database-native
  extract-load procedures when using a particular database as a
  source/target.

- Multi-thread and/or multi-process when speed/performance gains
  likely.

- Contextual transformation support. -->

## Project Plan, Milestones and Deliverables

There are no deliverables defined in this project: milestones are used in their place. Most of the milestones are structured around feedback rounds between the author and the supervisor. These feedback rounds are represented as red bars in the Gantt chart in [Figure @fig:gantt]. Once a first iteration of each work package has been completed and written up in the dissertation, it is submitted to the supervisor for review. The supervisor then has one week to respond with feedback on the work package. Similarly, feedback on the final dissertation is expected within two weeks of submission for feedback. See [Table @tbl:del] for the concrete dates of each milestone review.

```{.mermaid loc=img format=svg theme=neutral caption=gantt}
---
title: Gantt Chart of the activities defined for this project.
---
gantt
  todayMarker off
  section Project Setup
    Gather test and benchmark datasets: 2024-02-19, 18d
    Develop and integrate test framework: 10d
    M1 Supervisor feedback review:  crit, 1w
    Implement feedback: 1w
  section eel-yaml
    Create yaml specification and interpreter: 2024-03-18, 18d
    Run yaml specifications on test battery: 10d
    M2 Supervisor feedback review:  crit, 1w
    Implement feedback: 1w
  section Progress Report
    write: 2024-04-15, 3d
    M3 Submit: milestone, 0d
  section eel-project
    Create and implement yaml project/hierarchy: 2024-04-18, 15d
    Implement configuration creation function: 10d
    M4 Supervisor feedback review:  crit, 1w
    Implement feedback: 1w
  section eel-cli
    Integrate pipeline execution into cli: 2024-05-13, 18d
    Add preview functionalities: 10d
    M5 Supervisor feedback review:  crit, 1w
    Implement feedback: 1w
  section Dissertation
    write :2024-03-01, 5M
    M6 Supervisor feedback review: crit,2024-07-08, 2w
    M7 Submit :milestone, 2024-07-31, 0d
```

: List of project milestones. {#tbl:del}

| Mx  | Due 2024 | Description                  | Supervisor Response |
| --- | -------- | ---------------------------- | ------------------- |
| M1  | Mar-18   | Project Setup feedback round | +1 week             |
| M2  | Apr-15   | eel-yaml feedback round      | +1 week             |
| M3  | Apr-18   | Progress Report              |                     |
| M4  | May-15   | eel-project feedback round   | +1 week             |
| M5  | Jun-10   | eel-cli feedback round       | +1 week             |
| M6  | Jul-08   | Dissertation final review    | +2 weeks            |
| M7  | Jul-31   | Dissertation submitted       |                     |

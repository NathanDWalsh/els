## Motivation Should

Introduce the topic of research and explain its academic and industrial
context.

- Establish the general subject area.

- Describe the broad foundations of your study -- provide adequate
  background for readers.

- Indicate the general scope of your project.

## Introduction and Motivation

> Data pipelines are sets of processes that move and transform data from various sources to a destination where new value can be derived. [@pipelines_pocket, p. 1]

Data pipelines are an important part of today's data landscape, underpinning manual accounting processes to advanced LLMs. Although data pipelines are generally discussed as a component in the data engineering of big data systems, when discussing data pipelines we will consider the following tasks:

: Data Pipeline Use Cases {#tbl:table1}

| User           | Data goal/task  | Tech examples        |
| -------------- | --------------- | -------------------- |
| Business user  | Analyse         | Excel/Powerquery M   |
| Data scientist | Analyse         | Python/R             |
| Data engineer  | Prepare         | ETL tools, scripting |
| Database admin | Migrate, Backup | ETL tools, scripting |

As shown in [@tbl:table1] above

Already we see a range of use cases for the data pipeline, sometimes these use cases overlap. For example, a data scientist may begin working with data to create a data product and when completed hand it over to a data engineer for automating. In this example, the data engineer may be using python scripts to import source data into dataframes using pythong scripts, while the data engineer may use ETL tooling to define a pipeline which stores data on disk for later retrieval by the data product. This highlights one of the challanges with managing data pipelines: they are oftern built with varying toolsets that include one or more languages and/or toolsets.

To summarise, despite this rise in data's importance, there is lacking a general purpose language for describing a data pipeline and its transformations. This makes it difficult to have a common cross-system language for describing such pipelines to facilitate the swapping of one datasource for another. This can lead to a company being stuck with a vendor who's pipeline platform would be hard to move away from.

<!-- Some limitations of such systmes is that they are task-focused instead of data-focused. -->

Tabular

To fully develop a data pipleline language is a large undertaking. Doing some quick calculations, let's say there are 10 file formats + 10 databases leaving us with 20 different possible datastores. Just "converting" each of these stores to another leaves us with 400 simple transformations to support and test. This leaves out that transformations vary and some are so custom that to define them in simple terms may not be possible. Let's say that 20 different simple transformations can be defined, this leaves us with 8000 different possibilies to test.

Some examples of simple transformations are as follows. Here we will differenciate between contextual and noncontextual transformations [@pipelines_pocket, p. 106]:

- noncontextual
  - copy data
  - convert data
  - parse data
  - string functions
  - create column
    - fixed value
    - simple calculation
    - aggregate calculation
    - windowed calculation
  - drop column
  - filter
    - remove duplicates
  - self-join
  - obfuscate sensitive data
- contextual
  - simple join
  - multi join
  - windowed join
  - filter

Given the scope of such an undertaking, the below proposal will narrow the scope to the early part of the pipeline known as _data ingestion_. Data ingestion is the part of the pipeline which moves data from one datastore to another datastore, optionally applying noncontextual transformations.

Declarative data pipelines are not a novel concept [@ingestbase], however when implemented they are normally offered as part of a big data platforms that are integrated in a monolithic system (NiFi, ascend.io) and their realisation occurs in large cloud based infrastructure focusing on big data applications. This paper proposes the development of a declarative system for ingesting data that focuses on smaller data projects and ease of use.

TODO:

- add more refs
- Engage the readers.
- Provide an overview of the sections that will appear in your
  proposal (optional).

### Problem Statement Should

> The great virtue of a declarative language is that it makes the intent clear. You're not saying how to do something, you're saying what you want to achieve. [@patterns_eaa, p. 39]

- Answer the question: "What is the gap that needs to be filled?\"
  and/or "What is the problem that needs to be solved?\"

- State the problem clearly early in a paragraph.

- Limit the variables you address in stating your problem.

- Consider bordering the problem as a question.

### Problem Statement

The early stages of a data project often involve (insert refs CRISP-DM, DataONE, Data Engineering) a consolidation of multiple datasets into a single datastore or project spanning one or more tables. This paper will use the data engineering term _data ingestion_ to refer to this phase, concretely: the process of copying data from one or more sources to a single target. [Figure @fig:dataflow] below shows and example of a dataflow with multiple files landing in a single database scema, although the destination coud also be a python project or another set of files in a folder.

```{.mermaid loc=img format=svg theme=neutral caption=dataflow}
---
title: Example dataflow
---
flowchart LR
    subgraph fs:./data sources/
        a1[products.csv]
        subgraph transactions.xlsx/sheets/
            z1[january]
            y1[february]
        end
        subgraph customers.xlsx/sheets/
            x1[sold to]
            w1[ship to]
        end
    end
      subgraph db:target_db/tables/
          a1 --> b1[products]
          z1 --> d1[transactions]
          y1 --> d1
          x1 --> u1[sold to]
          w1 --> v1[ship to]
      end
```

Setting up the data ingestion process often involves many manual steps of data discovery, cleansing, standardisation and mapping to bring it into the target datastore. Depending on the project type the logic for these transformations can take various forms, from manually copy pasting data into an Excel spreadsheet to coding transformations in a python/pandas project. This makes communicating the steps taken to in this step a challange. It can also introduce reproducability issues when a decision to change a data definition, source or target while keeping the remainder of the logic consistent.

To summarise, the ingestion phase of a data project presents some challanges:

1. Manual logic defined that could otherwise be automated (mapping of fields, definition of datatypes)
2. System dependant: work done is not easily trasnferred to an alternative system (change in database backend, switching from in-memory analysis to datastore)
3. Lineage: steps taken to transfer data not easily descernable from differing logic by missing manual steps, decoding logic used in different systems

### Research Hypothesis and Objectives Should

Identify the overall aims of the project and the individual measurable
objectives against which you would wish the outcome of the work to be
assessed. Clearly spell out any research hypothesis you are following.

Include a justification (rationale) for the study. Be clear about what
your study will not address.

### Research Hypothesis and Objectives (not research project)

The project proposed below A platform agnostic configuration system/language that can describe data ingestion can alleviate some of the problems outlined above.

1. Minimal configuration: data introspection for detecting tables, fields and data types.
2. System agnostic: source and target datastores can easily be changed.
3. Lineage can be read directly from configration.

What is missing

When available as standalone solutions (meltano), they involve a complicated setup process and are built upon a deadware technology (singer). It could also be argued that the methods for extracting and loading of data are imperative, where the configuration must explictly state the connectors for extraction and loading of data.

This paper proposes a minimal-configuration solution for data ingestion: extracting from and loading to common data formats. that will perform introspection on data sources and destinations in order to

This paper proposes a project that will develop and implement a first phase of a project that will eventually be the first phase of a system for creating declarative pipelines. Focusing on the ingestion phase and common formats, the system could be extended to account for transformation phase.

### Timeliness and Novelty Should

Explain why the proposed research is of sufficient timeliness and
novelty

### Timeliness and Novelty

The proposed project is a re-evalulation of data trasnformation from the ground up. Starting with a base in the early stages of data extraction, it seeks to lay a common groundwork for a general data transformation syntax that can be used generally for any data task.

With the current hype of AI and LLMs, these are all based on good data and often large amounts thereof. This project could be the basis for...

#### Timeliness

In the current context of AI and LLM hype a la ChatGPT, this project is a back to basics re-think of how data pipelines can be generalized and communicated across differing platforms. With a good benchmark, AI tools can be further used to optimize some of the data transformation processes defined herein.

#### Novelty

Although the theory of a declarative ETL has been around for a while (insert some ref), there has not been a general purpose impementation. The proposal herein seeks to get such a system started by beginning with a basic framework beginning with some common data formats and databases (insert some ref). In section x we will propose a roadmap for further enhancements of this project to eventually scale.

### Significance Should

The proposal should demonstrate the originality of your intended
research. You should therefore explain why your research is important
(for example, by explaining how your research builds on and adds to the
current state of knowledge in the field or by setting out reasons why it
is timely to research your proposed topic) and providing details of any
immediate applications, including further research that might be done to
build on your findings.

### Significance

Much attention is paid to big data projects that feed into LLMs and AI models. It can be said that big data projects can be more and more fit into single node systems such as laptops, etc. It is for this reason that ...

### Feasibility should (be obvious?)

Comment on the feasibility of the research plans given its limited time
frame and resources. Outline your plans for a feasibility study before
starting e.g. major implementation work.

### Feasibility

Given the amount of data formats, databases and data stores available, adding to that the number of types of transformations. The first phase of this project must be minimaly scoped to demonstrate its usefulness as a concept.

### Beneficiaries should

Describe how the research will benefit other researchers in the field
and in related disciplines. What will be done to ensure that they can
benefit?

### Beneficiaries

This is more a practical project than a research project. However the tooling itself could be a benefit to researches if adopted as a means of defining the data used as part of the research process.

Otherwise the main beneficiaries should be the following:

- Data engineers who work on small to medium data pipelines can use this system to define their data ingestion or EtL (Extract, small-t/non-contexual transform, Load).
- Data scientists who want a common language to define their data sources outside of the code-base
- Data professionals who wish to migrate data from one format/database to another, with minimal effort

## Background and Related Work should

Demonstrate a knowledge and understanding of past and current work in
the subject area, including relevant references like this [@template].

## Background and Related Work

## Programme and ?Methodology? Method

- Detail the methodology to be used in pursuit of the research and
  justify this choice.

- Describe your contributions and novelty and where you will go beyond
  the state-of-the-art (new methods, new tools, new data, new
  insights, new proofs,\...)

- Describe the programme of work, indicating the research to be
  undertaken and the milestones that can be used to measure its
  progress.

- Where suitable define work packages and define the dependences
  between these work packages. WPs and their dependences should be
  shown in the Gantt chart in the research plan.

- Explain how the project will be managed.

- State the limitations of your research.

### Risk Assessment

### Ethics

## Evaluation

- Describe the specific methods of data collection.

- Explain how you intent to analyse and interpret the results.

## Expected Outcomes

Conclude your research proposal by addressing your predicted outcomes.
What are you hoping to prove/disprove? Indicate how you envisage your
research will contribute to debates and discussions in your particular
subject area:

- How will your research make an original contribution to knowledge?

- How might it fill gaps in existing work?

- How might it extend understanding of particular topics?

## Research Plan, Milestones and Deliverables

```{.mermaid loc=img format=svg theme=neutral caption=gantt}
---
title: Gantt Chart of the activities defined for this project.
---
gantt
  section IPP
    write: 2024-02-01, 18d
    supervisor review: crit, 1w
    revise: 1w
    majority agreed with supervisor  :milestone, 0d
    fine tuning with tutor: 3M
    submit :milestone,  0d
  section Benchmark<br>&<br>Test framework
    gather test and benchmark datasets: 2024-02-19, 18d
    setup CI/CT for tests: 10d
    supervisor review: crit, 1w
    implement feedback: 1w
  section Configuration<br>Language
    create yaml specification: 2024-03-18, 18d
    implement yaml specification and test on test/benchmark data: 10d
    supervisor review: crit, 1w
    implement feedback: 1w
  section Project<br>Structure
    implemnet yaml project hierarchy: 2024-04-15, 18d
    ease of use considerations: 10d
    supervisor review: crit, 1w
    implement feedback: 1w
  section CLI
    execution: 2024-05-13, 18d
    preview : 10d
    supervisor review: crit, 1w
    implement feedback: 1w
  section Dissertation
    write :2024-03-01, 5M
    submit for feedback: milestone, 2024-07-08, 0d
    supervisor review: crit, 2w
    feedback received: milestone , 0d
    submit :milestone, 2024-07-31, 0d
```

- revision / iteration

| Milestone | Week | Description                              |
| --------- | ---- | ---------------------------------------- |
| M1        | 2    | Feasibility study completed              |
| M2        | 5    | First prototype implementation completed |
| M3        | 7    | Evaluation completed                     |
| M4        | 10   | Submission of dissertation               |

: Milestones defined in this project.

| Deliverable | Week | Description                |
| ----------- | ---- | -------------------------- |
| D1          | 6    | Software tool for . . .    |
| D2          | 8    | Evaluation report on . . . |
| D3          | 10   | Dissertation               |

: List of deliverables defined in this project.

## Not in Template

These section are not in the template

### Proposal

The project proposed below A platform agnostic configuration system/language that can describe data ingestion can alleviate some of the problems outlined above.

1. Minimal configuration: data introspection for detecting tables, fields and data types.
2. System agnostic: source and target datastores can easily be changed.
3. Lineage can be read directly from configration.

What is missing

When available as standalone solutions (meltano), they involve a complicated setup process and are built upon a deadware technology (singer). It could also be argued that the methods for extracting and loading of data are imperative, where the configuration must explictly state the connectors for extraction and loading of data.

This paper proposes a minimal-configuration solution for data ingestion: extracting from and loading to common data formats. that will perform introspection on data sources and destinations in order to

This paper proposes a project that will develop and implement a first phase of a project that will eventually be the first phase of a system for creating declarative pipelines. Focusing on the ingestion phase and common formats, the system could be extended to account for transformation phase.

### Project

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

### Principal Goals

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

### Secondary Goals

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

### Roadmap

Although these three parts of the project are considered complimentary
and will be developed in concert, it is expected that they would
eventually split and evolve into separate projects in order to separate
the declarative language from any future interpreters that could be
developed to support it.

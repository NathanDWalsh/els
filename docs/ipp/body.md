## Motivation

> Data pipelines are sets of processes that move and transform data from various sources to a destination where new value can be derived. [@pipelines_pocket, p. 1]

Data pipelines are an important part of today's data landscape, underpinning a vast array of processes: from manually created ad-hoc reports in the form of spreadsheets to advanced LLMs powering ChatGPT. Their purpose is to make data available and accessible to data consumers.

A basic unit of a pipeline moves data from a source to a target while optionally _transforming_ the data in the process. _Transformations_ change the state of the data: some basic examples include cleaning, removing and summarizing data. A basic pipeline unit is illustrated in [Figure @fig:sequence0] from the perspective of a _pipeline engine_: the process which executes pipelines.

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

The first stage of a pipeline involves a consolidation of multiple datasets into a single target, we will use the term _ingestion_ to refer to this stage. [Figure @fig:dataflow] is an example of an ingestion phase of a dataflow demonstrating two source files being loaded to a single database. Each edge in [Figure @fig:dataflow] represents the sequence in [Figure @fig:sequence0].

```{.mermaid loc=img format=svg theme=neutral caption=dataflow}
---
title: Two source files flowing to a single database
---
graph LR
  subgraph ./data sources/
      a1[products.csv]
      subgraph sales.xlsx/sheets/
          z1[2020]
          y1[2021]
      end
  end
    subgraph sql://target/raw
        a1 --> b1[products]
        z1 --> d1[sales]
        y1 --> d1
    end
```

For the last ten years I have been freelancing as a data consultant, creating and running several data pipeline projects simultaneously with different clients. This motivated me to think about how data pipeline projects can be rapidly implemented, iterated and change traced. During this process I have settled on using the pandas library in Python to facilitate the _ingestion_ phase. The pandas Python library is popular with data scientists and at its core stores tabular datasets in a structure called a DataFrame. For the _ingestion_ phase the Python/pandas layer was the _pipeline engine_ illustrated in [Figure @fig:sequence0]. This generally functioned an ad-hoc solution but required a lot of manual tweaking of the code in order to accommodate exceptions and edge cases. This inspired me to try to capture the essence of _data ingestion_ in an external configuration that could be read, interpreted and executed by a Python pipeline engine.

The following sections will explore the idea of a pandas declarative data layer.

### Problem Statement

How can _data ingestion_&mdash;the process of moving data from multiple sources to a single target&mdash;be streamlined? Before answering this question, let's first examine how this is currently done in the context of a hypothetical python project with a single csv source and sql target.

Below are the specs in bullet form, followed by a code block for execution.

- source
  - **file:** customer.csv
  - **encoding:** iso-8859-1 encoding
- transformations
  - **none**
- target
  - **sql database:** sqlite:///my_database.db
  - **sql table:** customer
  - **if table exists:** replace

```python
import pandas as pd
from sqlalchemy import create_engine

# Create a connection to the database
engine = create_engine('sqlite:///my_database.db')

# Read the CSV file with Brazilian Portuguese encoding
df_customers = pd.read_csv('customer.csv', encoding='iso-8859-1')

# Write the data from the CSV file to the database
df_customers.to_sql('customer', engine, if_exists='replace', index=False)
```

This being a straightforward example we can quickly identify the source and the target for the customer dataset. The configuration for this _ingestion_ is hard-coded in our python script. Now imagine that the customer data comes in multiple csv files: one for each of Brazil's five regions.

```bash
customers/
|-- north.csv
|-- northeast.csv
|-- central-west.csv
|-- southeast.csv
|-- south.csv
```

In order to accommodate this change to the way that this data will be ingested we would either have to explicitly code each of the five regions, or iterate each file in the customers folder. Either way we loose visibility into what data is being ingested. Another pain arises if we decide to add the name of the file as a new column as a transformation.

Below we have a modified spec in bullet form: only the source files and transformations have been modified. We see how these minor changes causes a significant change in the code block that follows.

- source
  - **files:** customers/\*.csv (five files)
  - **encoding:** iso-8859-1
- transformations
  - add column containing base name of file (without .csv extension)
- target
  - **sql database:** sqlite:///my_database.db
  - **sql table:** customer
  - **if table exists:** replace

```python
import os
import pandas as pd
from sqlalchemy import create_engine

# Create a connection to the database
engine = create_engine('sqlite:///my_database.db')

# Specify the directory you want to import files from
directory = 'customers'

# Use os.listdir to get the list of files
files = os.listdir(directory)

# Filter the list to only include CSV files
csv_files = [f for f in files if f.endswith('.csv')]

# Loop over the CSV files and import each one into the database
for file in csv_files:
    # Read the CSV file with Brazilian Portuguese encoding
    file_path = os.path.join(directory, file)
    df_customers = pd.read_csv(file_path, encoding='iso-8859-1')

    # Get the file name without the extension
    file_name = os.path.splitext(file)[0]

    # Add the region column
    df_customers['region'] = file_name

    # Write the data from the CSV file to the database
    # For the first file imported, replace existing table
    #  On subsequent runs, append to the existing data
    # Assumes csv have same column names, orders and types
    if_exists = 'replace' if engine.has_table('customer') else 'append'

    df_customers.to_sql('customer', engine, if_exists=if_exists, index=False)
```

We see that relatively small changes to how the source is structured requires a big change to the code importing it. This is just giving a simple example with a single table of data and a few tweaks. If we consider a project with hundreds of sources, each with different settings we begin to see the challenges associated with coding and maintaining this in a python script.

<!-- This is problematic because the imperative logic for how the data is imported is scattered in code and can be difficult to maintain and change.

The initial inspiration for this project was born out of a having an easy way to use multiple files stored in a single directory as a source for a data pipeline.

- there lacks a standard way to quickly move multiple sources into a single destination, allowing for sensible defaults but allowing changes easily. -->

### Proposed Solution

To address the challenges proposed above, a solution that externalizes source and target data definitions in a pandas ingestion project is proposed below. In effect, what is being proposed is a pandas configuration layer which defines sources, simple transformations, and target data.

Let's show what this configuration might look like for the example given above:

```yaml
# ingest_config.yml
target:
  type: sqllite
  location: my_database.db
  table: customer
  if_exists: replace
source:
  type: csv
  encoding: iso-8859-1
  location: customers/*.csv
add_columns:
  region: _file_name_base
```

To execute the pipeline defined above, a simple shell command could be called

```bash
eel -execute ingest_config.yml
```

<!-- 1. Friendly and intuitive: should be easy for a user to get started without the requirement of setting up configuration files manually. This means that sensible defaults are taken from data introspection and type inference. Defaults can optionally be written to a configuration project where they can be modified if/when necessary.
2. Readable plain text configuration: All configuration in logical folder structure and with one or more plain text configuration files defining sources, transformations and target. -->

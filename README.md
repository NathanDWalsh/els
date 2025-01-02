# ELS

WIP

ELS (Extract-Load-Spec) is a cross-platform CLI for loading data.

## Install

```console
python -m pip install elspec
```

## Basic usage - autoloader

With zero configuration, ELS defaults to autoloader.
There must be [recognized data files](#supported-source-files) in the directory context from which ELS is run:
These are the source files to be loaded.
Each dataset recognized in the source files will be loaded to a separate pandas dataframe.
Once loaded, a preview is printed.

Run ELS in a directory containing supported files, and it will load all files to a set of pandas dataframes and print preview.

## Supported source files

- csv
- tsv
- Excel

## Configuration Components

Pipeline configurations define the dataflow between sources and targets,
including any transformations. These configurations must be defined in a
structured manner: it is the design of this configuration structure via
els's _configuration components_ that is covered in this chapter. The
human-readable design is covered in [Configuration Schema](#configuration-schema), explaining
a shallow YAML document schema for setting ingestion pipeline
configurations.

## **Configuration component overview**

The first column lists three node
components; the second column lists three node-level components which
when present, configure the nodes in the first column.

| Node component     | Node-level config.   | Configures...                               |
| ------------------ | -------------------- | ------------------------------------------- |
| Configuration file |                      |                                             |
| `*.els.yml`        |                      | one or more ingestion units.                |
| Source file        |                      |                                             |
| `*.csv`            |                      | source file with default configuration.     |
| `*.xlsx`           |                      |                                             |
|                    | Source-level config. |                                             |
| `*.csv`            | `*.csv.els.yml`      | source file with explicit configuration.    |
| `*.xlsx`           | `*.xlsx.els.yml`     |                                             |
| Directory          |                      | directory with default configuration.       |
|                    | Dir.-level config.   |                                             |
|                    | `_.els.yml`          | directory with explicit configuration.      |
|                    | Root-level config.   |                                             |
|                    | `__.els.yml`         | root directory with explicit configuration. |

The configuration components are a set of six filesystem objects
available for defining a pipeline. The six configuration components are
divided between three nodes, and three node-level configurations. Nodes
are analogous to directories and files in a filesystem, except that
there are two file types: configuration and source. Node-level
configurations are analogous to file attributes or permissions in a
filesystem, each defining the configuration of a particular node. An
overview of the six different configuration components are presented in
[@tbl:configtypes] and enumerated below. The three node components are:
(1) configuration file; (2) source file; and (3) directory. The source
file node can be configured with a (4) source-level configuration; and
the directory node can be configured with a (5) directory-level
configuration, and a (6) root-level configuration.

The components can be put together in a variety of ways, allowing for a
_configuration scheme_ to be built based on the requirements of the user
and/or project. A configuration scheme roughly defines what components
are chosen to configure an ingestion project. One example is a single
file configuration scheme where all sources, targets and transformations
are defined. Another example is a multiple-level configuration scheme
with multiple files and directories relying on configuration
inheritance. els does not favour nor enforce any particular
configuration scheme, instead it is up to the user to decide how to use
the components available.

With the aid of a series of examples, each component is explained in the
sections below. The components are not introduced in any logical order,
instead favouring an order that fits with the examples. The examples are
used to demonstrate the flexible design of the els system and are not
demonstrating recommended uses.

### Source File Node

Data files (csv, Excel) are interpreted by els as source file nodes,
allowing for the addition and removal of data files to a pipeline's
sources with simple file operations (copy, delete). This can be useful
for small projects with local datasets, or prototyping larger projects
with sample datasets. Source files added to a pipeline this way will be
ingested using defaults, when non-default configurations are required an
other configuration component can be employed.

[@lst:id100new] creates a new directory for the the running example, and
into it downloads two data files from the World Bank's public API.

```console
$ mkdir els-demo
C:\Users\nwals\els-demo
$ cd els-demo
$ $wb_api = "https://api.worldbank.org/v2/indicator/"
$ $wb_qry = "?downloadformat=excel"
$ curl -o ./Population.xls ($wb_api + "SP.POP.TOTL" + $wb_qry) -s
$ curl -o ./LabourForce.xls ($wb_api + "SL.TLF.TOTL.IN" + $wb_qry) -s
$ ls
C:\Users\nwals\els-demo\LabourForce.xls
C:\Users\nwals\els-demo\Population.xls
```

The last two lines of [@lst:id100new] show two data files that els will
recognise as _source file nodes_: `LabourForce.xls` and
`Population.xls`. [@lst:id108tree] introduces the `els tree` command,
which displays how els interprets a given configuration node and its
children. Passing a path as an argument to an els command sets the
_configuration context_, indicating where it should begin parsing
configuration components. In [@lst:id108tree] `els tree` is called,
setting the configuration context to the `Population.xls` file.

```console
$ els tree Population.xls
Population.xls
├── Data                  → memory['Data']
├── Metadata - Countries  → memory['Metadata - Countries']
└── Metadata - Indicators → memory['Metadata - Indicators']

```

The results of an `els tree` command always have: (1) a configuration
node as the root; (2) _source url nodes_ as penultimate nodes; and (3)
_dataflow nodes_ as leafs. The source url and dataflow nodes provide an
overview of the pipeline units defined in the configuration. Testing the
three enumerated points above against the results of [@lst:id108tree]:
The `Population.xls` root node is both (1) the configuration node, and
the (2) source url node; with three leafs as (3) dataflow nodes.

When reading an Excel file with default configuration, each sheet is
considered as a separate table. Since no target is set for this
pipeline, each target table is a pandas dataframes in memory.[^1]
Without explicit configuration, defaults are used for ingesting the
source. These defaults are overridden in the listings in
[@sec:directory-level-configuration] and
[@sec:source-level-configuration], but first the directory node is
described.

### Directory Node

Directory nodes are simply filesystem directories and can serve the same
organisational function. Used only for organising, they do not carry any
explicit configuration.[^2] A configuration scheme using directory nodes
can be employed to organise configuration components by project teams or
data providers.

[@lst:id110tree] calls the `els tree` command again, this time without
passing a path to set the context as done in [@lst:id108tree]. When
running els commands without passing an explicit path, the current
working directory is used for the configuration context.

```console
$ pwd
C:\Users\nwals\els-demo
$ els tree
els-demo
├── LabourForce.xls
│   ├── Data                  → memory['Data']
│   ├── Metadata - Countries  → memory['Metadata - Countries']
│   └── Metadata - Indicators → memory['Metadata - Indicators']
└── Population.xls
    ├── Data                  → memory['Data']
    ├── Metadata - Countries  → memory['Metadata - Countries']
    └── Metadata - Indicators → memory['Metadata - Indicators']
```

The `els tree` result in [@lst:id110tree] shows: the `els-demo`
directory node as the root; its children (`Population.xls` and
`LabourForce.xls`) as both source file nodes _and_ source url nodes; and
their children, the leafs, as dataflow nodes. The resulting dataflow
nodes show both files have three identical sheet names between them. The
pipeline as defined in [@lst:id110tree] would create three target
tables[^3] in memory, and append the contents of two source sheets into
each target.

Next, the directory-level configuration is introduced as a way to
configure directories and their contents.

### Directory-level Configuration

A feature of directory nodes is that they pass their configuration to
child nodes, analogous to how a filesystem directory's permissions are
passed to its child directories and files. To add configuration to a
directory node, a directory-level configuration file must be added. For
a directory-level configuration to be valid, it must: (1) be stored in
the same directory in which it configures; and (2) must be named
`_.els.yml`.[^4] Configurations set in a directory node are passed to
its child nodes via configuration inheritance.

[@lst:id120config] configures the `els-demo` directory node by creating
a directory-level configuration file[^5].

```console
$ pwd
C:\Users\nwals\els-demo
$ echo "source:"        >  _.els.yml
$ echo "  table: Data"  >> \_.els.yml
$ els tree
els-demo
├── LabourForce.xls
│   └── Data → memory['Data']
└── Population.xls
    └── Data → memory['Data']

```

The `els tree` results in [@lst:id120config] shows only the `Data` table
for both source files. This is because the `els-demo` directory node
passes the `source.table: Data` configuration to its child data files. A
directory-level configuration can be used is in configuration schemes
where different teams (geographic or departmental) are responsible for
the ingestion of their own datasets. In this scenario, each team is
responsible for a sub-directory which contains a directory-level
configuration file plus one or more configuration nodes.

Similar to how directory nodes can be configured, source file nodes can
also be configured with a source-level configuration, covered in the
next section.

### Source-level Configuration

When working directly with source file nodes covered in
[@sec:source-file-node], source-level configurations are a way to set
configurations for source data files. For a source-level configuration
to be valid, it must: (1) be in the same directory as the source data
file it configures; and (2) have the same base name as the source file
it configures, with a `.els.yml` extension added[^6].

Recall in the `els tree` dataflow results in [@lst:id120config] that the
`Data` sheets from both source files point to the same target `Data`
table in memory. [@lst:id122config] creates a source-level configuration
file for both source files, directing `Data` sheets each to a distinct
target table.

```console
$ echo "target:"                        > LabourForce.xls.els.yml
$ echo "  table: WorldBankLabourForce" >> LabourForce.xls.els.yml
$ echo "target:"                        > Population.xls.els.yml
$ echo "  table: WorldBankPopulation"  >> Population.xls.els.yml
$ els tree
els-demo
├── LabourForce.xls.els.yml
│   └── LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.els.yml
    └── Population.xls
        └── Data → memory['WorldBankPopulation']
```

The `els tree` results in [@lst:id122config] show two dataflow nodes
each with a distinct target table. Source-level configurations are
useful to quickly iterate configurations on a single source file. When a
desirable configuration is achieved, the configuration can be redirected
to a different configuration file or directory.

The examples so far used a configuration scheme which either uses source
data files directly, or mixes source data files with configuration
files. The next examples will use a configuration scheme that separates
configuration and source files into separate directories. This means
that both source file nodes and source-level configurations will not be
valid in this new configuration scheme. To set it up, [@lst:id130source]
moves the source files downloaded in [@lst:id100new] to a new `source`
directory and the configuration files created in [@lst:id120config] and
[@lst:id122config] to a new `config` directory.

```console
$ mkdir config
C:\Users\nwals\els-demo\config
$ mkdir source
C:\Users\nwals\els-demo\source
$ mv *.xls source
$ mv *.yml config
$ ls -s *.*
C:\Users\nwals\els-demo\config\_.els.yml
C:\Users\nwals\els-demo\config\LabourForce.xls.els.yml
C:\Users\nwals\els-demo\config\Population.xls.els.yml
C:\Users\nwals\els-demo\source\LabourForce.xls
C:\Users\nwals\els-demo\source\Population.xls
```

### Configuration File Node

So far only source-level and directory-level configurations have been
reviewed--those which configure their respective node. Configuration
file nodes, being nodes in the configuration hierarchy themselves,
define one or more ingestion units.[^7] For a configuration file node to
be valid, it must either: (1) define within itself a source url; or (2)
inherit a source url from one of its ancestor nodes.

Since separating the configuration files and source data files in
[@lst:id130source], the source-level configuration files created in
[@lst:id122config] are now considered by els as configuration file
nodes. However, they are invalid as configuration file nodes because
they do not have a source url defined. [@lst:id132source] resolves this
issue by adding a `source.url` property.

```console
$ cd config
$ echo "source:"                          >> LabourForce.xls.els.yml
$ echo "  url: ../source/LabourForce.xls" >> LabourForce.xls.els.yml
$ echo "source:"                          >> Population.xls.els.yml
$ echo "  url: ../source/Population.xls"  >> Population.xls.els.yml
$ els tree
config
├── LabourForce.xls.els.yml
│   └── LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.els.yml
    └── Population.xls
        └── Data → memory['WorldBankPopulation']
```

In [@lst:id132source] the results of the `els tree` command gives
similar results to [@lst:id122config] with two notable differences: (1)
the root node is now the newly created `config` directory node; and (2)
the second-level nodes are both configuration file nodes.

### Root-level Configuration

The root-level configuration has a similar function to the
directory-level configuration[^8], except that it also tags the
directory as a configuration root node. A root-level configuration is
analogous to a project's ini file, where project-wide settings are set.
Setting the root node of a configuration scheme has two benefits: (1)
sets pipeline or project-wide configurations; and (2) allows child nodes
of the scheme to be executed in isolation while keeping the inheritance
chain from the configuration root intact.

To contrast the behaviour between directory and root-level
configurations, a directory-level configuration is created in
[@lst:id142root] and renamed to a root-level configuration in
[@lst:id146root]. To set up the next examples, [@lst:id140root] creates
a directory for the World Bank configuration files and moves them there.

```console
$ mkdir world_bank
C:\Users\nwals\els-demo\config\world_bank
$ mv *._ world_bank
$ ls -s _.\_
C:\Users\nwals\els-demo\config\world_bank\_.els.yml
C:\Users\nwals\els-demo\config\world_bank\LabourForce.xls.els.yml
C:\Users\nwals\els-demo\config\world_bank\Population.xls.els.yml

```

[@lst:id140root] demonstrates a configuration scheme where
configurations are segregated by source provider, albeit only one (World
Bank). In [@lst:id142root], a directory-level configuration is created
in the `config` directory to explicitly set a `target.url` for the
pipeline. This replaces the default `memory` target that has been
observed up to now.

```{#id142root .console caption="Explicitly set a target for the pipeline, using a directory-level config"}
$ pwd
C:\Users\nwals\els-demo\config
$ echo "target:"                 >  _.els.yml
$ echo "  url: ../target/*.csv"  >> _.els.yml
$ els tree
config
└── world_bank
    ├── LabourForce.xls.els.yml
    │   └── LabourForce.xls
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xls.els.yml
        └── Population.xls
            └── Data → ..\target\WorldBankPopulation.csv
```

In [@lst:id142root], the results of `els tree`, executed in the context
of the `config` directory, show the targets as csv files, consistent
with the configuration set above. [@lst:id144root] runs `els tree`
again, but this time in context of the `world_bank` directory.

```console
$ els tree ./world_bank/
world_bank
├── LabourForce.xls.els.yml
│   └── ../source/LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.els.yml
    └── ../source/Population.xls
        └── Data → memory['WorldBankPopulation']

```

The results of [@lst:id144root] show the targets defaulted back to
`memory`. Since `els tree` was run in the context of the `world_bank`
directory, it uses this as the root node from which to grow the tree.

When project-wide settings are desirable, a root-level configuration
should be created in the desired root directory. Configuration root
directories are searched for in ancestor directories when els commands
are run. If found, els ensures the configuration chain is maintained
between the root-level node and configuration context of the els
command. This is a convenient way for components of a pipeline to be
executed in isolation while maintaining project-wide configurations that
are set in the root.

Recall in [@lst:id142root] that a _directory_-level configuration was
created--not a _root_-level configuration. For a root-level
configuration to be valid, it must: (1) be stored in the same directory
destined to be the root node; and (2) must be named `__.els.yml`. To
make the `config` directory a root configuration directory, the
directory-level configuration file created in [@lst:id142root] is
renamed to a root-level configuration file in [@lst:id146root].

```console
$ ren _.els.yml \_\_.els.yml
$ ls -s _._
C:\Users\nwals\els-demo\config\world_bank\_.els.yml
C:\Users\nwals\els-demo\config\world_bank\LabourForce.xls.els.yml
C:\Users\nwals\els-demo\config\world_bank\Population.xls.els.yml
C:\Users\nwals\els-demo\config\_\_.els.yml
$ els tree ./world_bank/
config
└── world_bank
    ├── LabourForce.xls.els.yml
    │   └── LabourForce.xls
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xls.els.yml
        └── Population.xls
            └── Data → ..\target\WorldBankPopulation.csv

```

The results of the `els tree` command in [@lst:id146root] reflects the
target set in the `./config/__.els.yml` ([@lst:id142root]) even though
`els tree` was executed in the context of the `./config/world_bank/`
node. This is because els searches in ancestor directories of the
execution context until it identifies a root-level configuration. When
found, it ensures the configuration chain between the found root node
and the configuration context is intact. els is only interested in
maintaining the configuration nodes _between_ the found configuration
root and the execution context, ignoring other sub-directories in
between. From the configuration context, descendant nodes are traversed
as usual.

Having reviewed the six different configuration components in the els
toolbox, this chapter will be concluded with a section on
multiple-documents before closing with a summary.

### Multiple-document Configuration Files

YAML files can have more than one YAML document separated by a `---`
line; likewise els configuration files can have more then one document.
To demonstrate a multiple-document configuration file, a configuration
file node is created to replace the three configuration files from the
`world_bank` directory node.

[@lst:id148multi] creates a `world_bank.els.yml` configuration node
which can replace the `world_bank` directory node from
[@lst:id146root].[^9]

```console
$ pwd
C:\Users\nwals\els-demo\config
$ echo "target:"                          >  world_bank.els.yml
$ echo "  table: WorldBankLabourForce"    >> world_bank.els.yml
$ echo "source:"                          >> world_bank.els.yml
$ echo "  url: ../source/LabourForce.xls" >> world_bank.els.yml
$ echo "  table: Data"                    >> world_bank.els.yml
$ echo "---"                              >> world_bank.els.yml
$ echo "target:"                          >> world_bank.els.yml
$ echo "  table: WorldBankPopulation"     >> world_bank.els.yml
$ echo "source:"                          >> world_bank.els.yml
$ echo "  url: ../source/Population.xls"  >> world_bank.els.yml
$ echo "  table: Data"                    >> world_bank.els.yml
$ els tree world_bank.els.yml
config
└── world_bank.els.yml
    ├── LabourForce.xls
    │   └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xls
        └── Data → ..\target\WorldBankPopulation.csv

```

The results of the `els tree` command in [@lst:id148multi] shows the
same effective dataflow configuration from [@lst:id146root]. One
difference is that it has two less mid-level configuration file nodes,
reflecting the change in configuration scheme. The configuration scheme
from this example has each source data provider (World Bank) given its
own configuration file node. This pattern could even be extended further
to a _single configuration file scheme_, removing the need for a
root-level configuration and keeping all configuration in a single
configuration node.

## Configuration Schema

A YAML schema is a file that defines the property structure of YAML
files. Likewise, els comes with a _configuration file schema_ that
defines the configuration files reviewed in [Configuration Components](#configuration-components).

els configuration files are YAML files with a `.els.yml` extension,
which define properties of an ingestion pipeline such as data sources,
targets and transformations. When els reads a configuration file, it
first validates it against the configuration file schema. This
validation may also be performed by other applications such as vs.code.
Using such a code editor as vs.code, the user benefits from real-time
validation _and_ autocompletion. Code editors can stand-in as a user
interface for authoring configuration files, allowing a user to create,
modify and validate configuration files rapidly.

Returning to the running example, the `els preview` command is
introduced in [@lst:id200preview]. `els preview` displays a sample of
rows and columns for each _target table_ defined in the pipeline. Like
the `els tree` command reviewed in [Configuration Components](#configuration-components),
`els preview` requires a valid configuration context, using the current
working directory as a default. To keep the listings as brief as
possible by previewing a single source file, the listings in the
following examples will continue working from the `world_bank` directory
node from [@lst:id146root].

[@lst:id200preview] calls the `els preview` command passing the
`Population.xls.els.yml` configuration node.

```console
$ cd world_bank
$ els preview Population.xls.els.yml
WorldBankPopulation [269 rows x 68 columns]:
         Data Source World Development Indicators  ... Unnamed: 66 Unnamed: 67
0  Last Updated Date  2024-06-28 00:...            ...         NaN         NaN
1                NaN                NaN            ...         NaN         NaN
2       Country Name       Country Code            ...      2022.0      2023.0
3              Aruba                ABW            ...    106445.0    106277.0

```

### Read Configuration

When loading an Excel sheet (or csv file) with the default
configuration, the first row is assumed the be for column names. The
result in [@lst:id200preview] has many unnamed columns because the
header row should be set to the fourth row.

els uses pandas reader functions (`read_sql`, `read_csv`, `read_excel`)
to read source data. Configuration properties under `source.read_*` are
passed to the respective pandas functions as arguments, enabling els to
benefit from the features of the pandas reader functions.
[@lst:id210skprows] sets the `source.read_excel.skiprows` property in
the configuration.

```console
$ echo "  read_excel:"    >> Population.xls.els.yml
$ echo "    skiprows: 3"  >> Population.xls.els.yml
$ cat Population.xls.els.yml
target:
  table: WorldBankPopulation
source:
  url: ../source/Population.xls
  read_excel:
    skiprows: 3
$ els preview Population.xls.els.yml
WorldBankPopulation [266 rows x 68 columns]:
        Country Name Country Code  ...         2022         2023
0              Aruba          ABW  ...     106445.0     106277.0
1  Africa Eastern...          AFE  ...  720859132.0  739108306.0
2        Afghanistan          AFG  ...   41128771.0   42239854.0
3  Africa Western...          AFW  ...  490330870.0  502789511.0

```

The results of `els preview` in [@lst:id210skprows] show the header set
correctly. However the last two columns in [@lst:id210skprows] show
years 2022 and 2023 as column names, with the population figure as
values under each year column. A solution to this is applied in the next
section.

### Transformations

The year columns can be reshaped to rows, or normalised, using the
pandas `melt` function. Some pandas transformations like `melt` and
`stack` are available as transformations in els. These transformations
are defined under the root `transform` property in a configuration file.
Like the `read_*` functions, sub-properties set under a `transform.*`
property are passed as arguments to the respective pandas function.

In [@lst:id220transform], `transform.melt` is added with sub-properties
to pass to the pandas `melt` function.

```console
$ echo "transform:"                   >> Population.xls.els.yml
$ echo "  melt:"                      >> Population.xls.els.yml
$ echo "    id_vars:"                 >> Population.xls.els.yml
$ echo "      - Country Name"         >> Population.xls.els.yml
$ echo "      - Country Code"         >> Population.xls.els.yml
$ echo "      - Indicator Name"       >> Population.xls.els.yml
$ echo "      - Indicator Code"       >> Population.xls.els.yml
$ echo "    value_name: Population"   >> Population.xls.els.yml
$ echo "    var_name: Year"           >> Population.xls.els.yml
$ els preview Population.xls.els.yml
WorldBankPopulation [17024 rows x 6 columns]:
        Country Name Country Code  ...  Year   Population
0              Aruba          ABW  ...  1960      54608.0
1  Africa Eastern...          AFE  ...  1960  130692579.0
2        Afghanistan          AFG  ...  1960    8622466.0
3  Africa Western...          AFW  ...  1960   97256290.0

```

The results of the `els preview` command in [@lst:id220transform] show
the year columns have been reshaped to rows.

However note that the figures in the `Population` column each have a
`.0` after them: this is because pandas is reading this column as a
`float`. In [@lst:id225transform], the population column is changed to
data type `Int64`[^10] by setting the `transform.astype.dtype` property.

```console
$ echo "  astype:"                    >> Population.xls.els.yml
$ echo "    dtype:"                   >> Population.xls.els.yml
$ echo "      Population: Int64"      >> Population.xls.els.yml
$ els preview Population.xls.els.yml
WorldBankPopulation [17024 rows x 6 columns]:
        Country Name Country Code  ...  Year Population
0              Aruba          ABW  ...  1960      54608
1  Africa Eastern...          AFE  ...  1960  130692579
2        Afghanistan          AFG  ...  1960    8622466
3  Africa Western...          AFW  ...  1960   97256290

```

The `els preview` results in [@lst:id225transform] show the `Population`
column as integers. Having reviewed some transformations, the next
section demonstrates adding columns.

### Adding Columns

The `add_cols` root property of the configuration is used for adding
columns during pipeline execution. Members of `add_cols` are dictionary
entries where the _dictionary key_ is name of the column and the
_dictionary value_ is the value of the column.

A related feature is the ability to add columns with _dynamic enum_
values which are calculated at runtime. Dynamic enums are enums in the
configuration schema, and evaluated by els during pipeline
execution.[^11] Most of these dynamic enums currently available relate
to file metadata such as file name, parent directory, etc.

In [@lst:id235addcols], two columns are added: (1) `Date Downloaded`
with fixed scaler value `2024-07-16`; and (2) `Source File` with the
dynamic enum `_file_name_full`.

```{#id235addcols .console caption="Add new columns."}
$ echo "add_cols:"                       >> Population.xls.els.yml
$ echo "  Date Downloaded: 2024-07-16"   >> Population.xls.els.yml
$ echo "  Source File: _file_name_full"  >> Population.xls.els.yml
$ els preview Population.xls.els.yml
WorldBankPopulation [17024 rows x 8 columns]:
        Country Name Country Code  ... Date Downloaded     Source File
0              Aruba          ABW  ...      2024-07-16  Population.xls
1  Africa Eastern...          AFE  ...      2024-07-16  Population.xls
2        Afghanistan          AFG  ...      2024-07-16  Population.xls
3  Africa Western...          AFW  ...      2024-07-16  Population.xls
```

The results of `els preview` in [@lst:id235addcols] reflect the two new
columns defined above. Having reviewed the properties of the
configuration schema, next the configuration class that implements the
configuration schema is reviewed.

[^1]:
    This is useful for data science projects where an els
    configuration can be used to define data sources only. This use case
    requires using the els library directly in a Python script and is
    beyond the scope of this paper.

[^2]:
    Though they may inherit configuration from ancestor directories.
    Configuration inheritance will be explained in more detail in
    [@sec:directory-level-configuration].

[^3]: pandas dataframes.
[^4]:
    A detailed explanation of the YAML configuration files and its
    schema is provided in [@sec:els-config-design].

[^5]:
    The listings in this paper use redirection operators `>` to
    create a configuration file, and `>>` to append to a configuration
    file. However it can be more convenient to use a code editor as
    suggested in [@sec:els-config-design].

[^6]:
    Column two in the source file section of [@tbl:configtypes]
    demonstrates this pattern.

[^7]:
    Possibly with the help of configuration inheritance from parent
    directory nodes.

[^8]: [@sec:directory-level-configuration].
[^11]:
    Here is demonstrated the use of dynamic enums in the context of
    adding columns, however they can be used as any value in any
    property in the configuration schema. This is an advanced usage and
    beyond the scope of this paper.


```{.console #id235addcols caption="Add new columns."}
$ echo "add_cols:"                       >> Population.xlsx.eel.yml
$ echo "  Date Downloaded: 2024-07-16"   >> Population.xlsx.eel.yml
$ echo "  Source File: _file_name_full"  >> Population.xlsx.eel.yml
$ eel preview Population.xlsx.eel.yml
WorldBankPopulation [256 rows x 8 columns]:
        Country Name Country Code  ... Date Downloaded      Source File
0              Aruba          ABW  ...      2024-07-16  Population.xlsx
1  Africa Eastern...          AFE  ...      2024-07-16  Population.xlsx
2        Afghanistan          AFG  ...      2024-07-16  Population.xlsx
3  Africa Western...          AFW  ...      2024-07-16  Population.xlsx
```

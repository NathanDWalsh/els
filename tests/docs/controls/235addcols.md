
```{.console #id235addcols caption="Add new columns."}
$ echo "add_cols:"                       >> Population.xls.eel.yml
$ echo "  Date Downloaded: 2024-07-16"   >> Population.xls.eel.yml
$ echo "  Source File: _file_name_full"  >> Population.xls.eel.yml
$ eel preview Population.xls.eel.yml
WorldBankPopulation [17024 rows x 8 columns]:
        Country Name Country Code  ... Date Downloaded     Source File
0              Aruba          ABW  ...      2024-07-16  Population.xls
1  Africa Eastern...          AFE  ...      2024-07-16  Population.xls
2        Afghanistan          AFG  ...      2024-07-16  Population.xls
3  Africa Western...          AFW  ...      2024-07-16  Population.xls
```

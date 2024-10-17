
```{.console #id235addcols caption="Add new columns."}
$ echo "add_cols:"                       >> Population.xls.els.yml
$ echo "  Date Downloaded: 2024-07-16"   >> Population.xls.els.yml
$ echo "  Source File: _file_name_full"  >> Population.xls.els.yml
$ els preview Population.xls.els.yml
WorldBankPopulation [17024 rows x 8 columns]:
        Country Name Country Code  ... Date Downloaded     Source File
0              Aruba          ABW  ...      2024-07-16  Population.xls
1  Africa Eastern...          AFE  ...      2024-07-16  Population.xls
2        Afghanistan          AFG  ...      2024-07-16  Population.xls
3  Africa Western...          AFW  ...      2024-07-16  Population.xls
```

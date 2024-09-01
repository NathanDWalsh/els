
```{.console #id230addcols caption="Add a column including the date the data was downloaded"}
$ echo "add_cols:"                      >> Population.xls.eel.yml
$ echo "  Date Downloaded: 2024-07-16"  >> Population.xls.eel.yml
$ eel preview Population.xls.eel.yml
WorldBankPopulation [17024 rows x 7 columns]:
        Country Name Country Code  ... Population Date Downloaded
0              Aruba          ABW  ...      54608      2024-07-16
1  Africa Eastern...          AFE  ...  130692579      2024-07-16
2        Afghanistan          AFG  ...    8622466      2024-07-16
3  Africa Western...          AFW  ...   97256290      2024-07-16
```

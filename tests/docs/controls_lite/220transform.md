
```{.console #id220transform caption="Add `transform.melt` to config file"}
$ echo "transform:"                   >> Population.xls.eel.yml
$ echo "  melt:"                      >> Population.xls.eel.yml
$ echo "    id_vars:"                 >> Population.xls.eel.yml
$ echo "      - Country Name"         >> Population.xls.eel.yml
$ echo "      - Country Code"         >> Population.xls.eel.yml
$ echo "      - Indicator Name"       >> Population.xls.eel.yml
$ echo "      - Indicator Code"       >> Population.xls.eel.yml
$ echo "    value_name: Population"   >> Population.xls.eel.yml
$ echo "    var_name: Year"           >> Population.xls.eel.yml
$ eel preview Population.xls.eel.yml
WorldBankPopulation [17024 rows x 6 columns]:
        Country Name Country Code  ...  Year   Population
0              Aruba          ABW  ...  1960      54608.0
1  Africa Eastern...          AFE  ...  1960  130692579.0
2        Afghanistan          AFG  ...  1960    8622466.0
3  Africa Western...          AFW  ...  1960   97256290.0
```

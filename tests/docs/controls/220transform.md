
```{.console #id220transform caption="Add `transform.melt` to config file"}
$ echo "transform:"                   >> Population.xls.els.yml
$ echo "  melt:"                      >> Population.xls.els.yml
$ echo "    id_vars:"                 >> Population.xls.els.yml
$ echo "      - Country Name"         >> Population.xls.els.yml
$ echo "      - Country Code"         >> Population.xls.els.yml
$ echo "      - Indicator Name"       >> Population.xls.els.yml
$ echo "      - Indicator Code"       >> Population.xls.els.yml
$ echo "    value_name: Population"   >> Population.xls.els.yml
$ echo "    var_name: Year"           >> Population.xls.els.yml
$ els preview Population.xls.els.yml
WorldBankPopulation [17024 rows x 6 columns]:
        Country Name Country Code  ...  Year   Population
0              Aruba          ABW  ...  1960      54608.0
1  Africa Eastern...          AFE  ...  1960  130692579.0
2        Afghanistan          AFG  ...  1960    8622466.0
3  Africa Western...          AFW  ...  1960   97256290.0
```

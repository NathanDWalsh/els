
```{.console #id220transform caption="Add `transform.melt` to config file"}
$ echo "transform:"                   >> Population.xlsx.eel.yml
$ echo "  melt:"                      >> Population.xlsx.eel.yml
$ echo "    id_vars:"                 >> Population.xlsx.eel.yml
$ echo "      - Country Name"         >> Population.xlsx.eel.yml
$ echo "      - Country Code"         >> Population.xlsx.eel.yml
$ echo "      - Indicator Name"       >> Population.xlsx.eel.yml
$ echo "      - Indicator Code"       >> Population.xlsx.eel.yml
$ echo "    value_name: Population"   >> Population.xlsx.eel.yml
$ echo "    var_name: Year"           >> Population.xlsx.eel.yml
$ eel preview Population.xlsx.eel.yml
WorldBankPopulation [256 rows x 6 columns]:
        Country Name Country Code  ...  Year Population
0              Aruba          ABW  ...  1960      54608
1  Africa Eastern...          AFE  ...  1960  130692579
2        Afghanistan          AFG  ...  1960    8622466
3  Africa Western...          AFW  ...  1960   97256290
```


```{.console #id225transform caption="Add `transform.astype` to config file"}
$ echo "  astype:"                    >> Population.xls.els.yml
$ echo "    dtype:"                   >> Population.xls.els.yml
$ echo "      Population: Int64"      >> Population.xls.els.yml
$ els preview Population.xls.els.yml
WorldBankPopulation [17024 rows x 6 columns]:
        Country Name Country Code  ...  Year Population
0              Aruba          ABW  ...  1960      54608
1  Africa Eastern...          AFE  ...  1960  130692579
2        Afghanistan          AFG  ...  1960    8622466
3  Africa Western...          AFW  ...  1960   97256290
```


```{.console #id225transform caption="Add `transform.astype` to config file"}
$ echo "  astype:"                    >> Population.xlsx.els.yml
$ echo "    dtype:"                   >> Population.xlsx.els.yml
$ echo "      Population: Int64"      >> Population.xlsx.els.yml
$ els preview Population.xlsx.els.yml
WorldBankPopulation [256 rows x 6 columns]:
        Country Name Country Code  ...  Year Population
0              Aruba          ABW  ...  1960      54608
1  Africa Eastern...          AFE  ...  1960  130692579
2        Afghanistan          AFG  ...  1960    8622466
3  Africa Western...          AFW  ...  1960   97256290
```

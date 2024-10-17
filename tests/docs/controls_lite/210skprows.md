
```{.console #id210skprows caption="Set the `source.read_excel.skiprows` attribute"}
$ echo "  read_excel:"    >> Population.xlsx.els.yml
$ echo "    skiprows: 3"  >> Population.xlsx.els.yml
$ cat Population.xlsx.els.yml
target:
  table: WorldBankPopulation
source:
  url: ../source/Population.xlsx
  read_excel:
    skiprows: 3
$ els preview Population.xlsx.els.yml
WorldBankPopulation [4 rows x 68 columns]:
        Country Name Country Code  ...       2022       2023
0              Aruba          ABW  ...     106445     106277
1  Africa Eastern...          AFE  ...  720859132  739108306
2        Afghanistan          AFG  ...   41128771   42239854
3  Africa Western...          AFW  ...  490330870  502789511
```


```{.console #id210skprows caption="Set the `source.read_excel.skiprows` attribute"}
$ echo "  read_excel:"    >> Population.xls.eel.yml
$ echo "    skiprows: 3"  >> Population.xls.eel.yml
$ cat Population.xls.eel.yml
target:
  table: WorldBankPopulation
source:
  url: ../source/Population.xls
  read_excel:
    skiprows: 3
$ eel preview Population.xls.eel.yml
WorldBankPopulation [266 rows x 68 columns]:
        Country Name Country Code  ...         2022         2023
0              Aruba          ABW  ...     106445.0     106277.0
1  Africa Eastern...          AFE  ...  720859132.0  739108306.0
2        Afghanistan          AFG  ...   41128771.0   42239854.0
3  Africa Western...          AFW  ...  490330870.0  502789511.0
```

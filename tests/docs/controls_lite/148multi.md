
```{.console #id148multi caption="Create multi-document `world_bank.eel.yml` file."}
$ pwd
C:\Users\nwals\eel-demo\config
$ echo "target:"                          >  world_bank.eel.yml
$ echo "  table: WorldBankLabourForce"    >> world_bank.eel.yml
$ echo "source:"                          >> world_bank.eel.yml
$ echo "  url: ../source/LabourForce.xlsx" >> world_bank.eel.yml
$ echo "  table: Data"                    >> world_bank.eel.yml
$ echo "---"                              >> world_bank.eel.yml
$ echo "target:"                          >> world_bank.eel.yml
$ echo "  table: WorldBankPopulation"     >> world_bank.eel.yml
$ echo "source:"                          >> world_bank.eel.yml
$ echo "  url: ../source/Population.xlsx"  >> world_bank.eel.yml
$ echo "  table: Data"                    >> world_bank.eel.yml
$ eel tree world_bank.eel.yml
config
└── world_bank.eel.yml
    ├── LabourForce.xlsx
    │   └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xlsx
        └── Data → ..\target\WorldBankPopulation.csv
```

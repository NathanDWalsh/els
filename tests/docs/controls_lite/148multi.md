
```{.console #id148multi caption="Create multi-document `world_bank.els.yml` file."}
$ pwd
C:\Users\nwals\els-demo\config
$ echo "target:"                          >  world_bank.els.yml
$ echo "  table: WorldBankLabourForce"    >> world_bank.els.yml
$ echo "source:"                          >> world_bank.els.yml
$ echo "  url: ../source/LabourForce.xlsx" >> world_bank.els.yml
$ echo "  table: Data"                    >> world_bank.els.yml
$ echo "---"                              >> world_bank.els.yml
$ echo "target:"                          >> world_bank.els.yml
$ echo "  table: WorldBankPopulation"     >> world_bank.els.yml
$ echo "source:"                          >> world_bank.els.yml
$ echo "  url: ../source/Population.xlsx"  >> world_bank.els.yml
$ echo "  table: Data"                    >> world_bank.els.yml
$ els tree world_bank.els.yml
config
└── world_bank.els.yml
    ├── LabourForce.xlsx
    │   └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xlsx
        └── Data → ..\target\WorldBankPopulation.csv
```

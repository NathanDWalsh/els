
```{.console #id142root caption="Explicitly set a target for the pipeline, using a directory-level config"}
$ pwd
C:\Users\nwals\els-demo\config
$ echo "target:"                 >  _.els.yml
$ echo "  url: ../target/*.csv"  >> _.els.yml
$ els tree
config
└── world_bank
    ├── LabourForce.xls.els.yml
    │   └── LabourForce.xls
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xls.els.yml
        └── Population.xls
            └── Data → ..\target\WorldBankPopulation.csv
```

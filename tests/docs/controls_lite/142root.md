
```{.console #id142root caption="Explicitly set a target for the pipeline, using a directory-level config"}
$ pwd
C:\Users\nwals\eel-demo\config
$ echo "target:"                 >  _.eel.yml
$ echo "  url: ../target/*.csv"  >> _.eel.yml
$ eel tree
config
└── world_bank
    ├── LabourForce.xlsx.eel.yml
    │   └── LabourForce.xlsx
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xlsx.eel.yml
        └── Population.xlsx
            └── Data → ..\target\WorldBankPopulation.csv
```

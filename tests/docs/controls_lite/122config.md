
```{.console #id122config caption="Create a source-level config for each source file"}
$ echo "target:"                        > LabourForce.xls.eel.yml
$ echo "  table: WorldBankLabourForce" >> LabourForce.xls.eel.yml
$ echo "target:"                        > Population.xls.eel.yml
$ echo "  table: WorldBankPopulation"  >> Population.xls.eel.yml
$ eel tree
eel-demo
├── LabourForce.xls.eel.yml
│   └── LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.eel.yml
    └── Population.xls
        └── Data → memory['WorldBankPopulation']
```

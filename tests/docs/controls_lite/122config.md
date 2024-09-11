
```{.console #id122config caption="Create a source-level config for each source file"}
$ echo "target:"                        > LabourForce.xlsx.eel.yml
$ echo "  table: WorldBankLabourForce" >> LabourForce.xlsx.eel.yml
$ echo "target:"                        > Population.xlsx.eel.yml
$ echo "  table: WorldBankPopulation"  >> Population.xlsx.eel.yml
$ eel tree
eel-demo
├── LabourForce.xlsx.eel.yml
│   └── LabourForce.xlsx
│       └── Data → memory['WorldBankLabourForce']
└── Population.xlsx.eel.yml
    └── Population.xlsx
        └── Data → memory['WorldBankPopulation']
```

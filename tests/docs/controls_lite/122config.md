
```{.console #id122config caption="Create a source-level config for each source file"}
$ echo "target:"                        > LabourForce.xlsx.els.yml
$ echo "  table: WorldBankLabourForce" >> LabourForce.xlsx.els.yml
$ echo "target:"                        > Population.xlsx.els.yml
$ echo "  table: WorldBankPopulation"  >> Population.xlsx.els.yml
$ els tree
els-demo
├── LabourForce.xlsx.els.yml
│   └── LabourForce.xlsx
│       └── Data → memory['WorldBankLabourForce']
└── Population.xlsx.els.yml
    └── Population.xlsx
        └── Data → memory['WorldBankPopulation']
```

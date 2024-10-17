
```{.console #id122config caption="Create a source-level config for each source file"}
$ echo "target:"                        > LabourForce.xls.els.yml
$ echo "  table: WorldBankLabourForce" >> LabourForce.xls.els.yml
$ echo "target:"                        > Population.xls.els.yml
$ echo "  table: WorldBankPopulation"  >> Population.xls.els.yml
$ els tree
els-demo
├── LabourForce.xls.els.yml
│   └── LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.els.yml
    └── Population.xls
        └── Data → memory['WorldBankPopulation']
```

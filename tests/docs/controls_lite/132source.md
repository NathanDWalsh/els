
```{.console #id132source caption="Add a source url to the config files"}
$ cd config
$ echo "source:"                          >> LabourForce.xlsx.els.yml
$ echo "  url: ../source/LabourForce.xlsx" >> LabourForce.xlsx.els.yml
$ echo "source:"                          >> Population.xlsx.els.yml
$ echo "  url: ../source/Population.xlsx"  >> Population.xlsx.els.yml
$ els tree
config
├── LabourForce.xlsx.els.yml
│   └── LabourForce.xlsx
│       └── Data → memory['WorldBankLabourForce']
└── Population.xlsx.els.yml
    └── Population.xlsx
        └── Data → memory['WorldBankPopulation']
```


```{.console #id132source caption="Add a source url to the config files"}
$ cd config
$ echo "source:"                          >> LabourForce.xls.els.yml
$ echo "  url: ../source/LabourForce.xls" >> LabourForce.xls.els.yml
$ echo "source:"                          >> Population.xls.els.yml
$ echo "  url: ../source/Population.xls"  >> Population.xls.els.yml
$ els tree
config
├── LabourForce.xls.els.yml
│   └── LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.els.yml
    └── Population.xls
        └── Data → memory['WorldBankPopulation']
```

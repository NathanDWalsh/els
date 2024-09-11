
```{.console #id132source caption="Add a source url to the config files"}
$ cd config
$ echo "source:"                          >> LabourForce.xls.eel.yml
$ echo "  url: ../source/LabourForce.xls" >> LabourForce.xls.eel.yml
$ echo "source:"                          >> Population.xls.eel.yml
$ echo "  url: ../source/Population.xls"  >> Population.xls.eel.yml
$ eel tree
config
├── LabourForce.xls.eel.yml
│   └── LabourForce.xls
│       └── Data → memory['WorldBankLabourForce']
└── Population.xls.eel.yml
    └── Population.xls
        └── Data → memory['WorldBankPopulation']
```

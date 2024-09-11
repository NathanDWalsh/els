
```{.console #id132source caption="Add a source url to the config files"}
$ cd config
$ echo "source:"                          >> LabourForce.xlsx.eel.yml
$ echo "  url: ../source/LabourForce.xlsx" >> LabourForce.xlsx.eel.yml
$ echo "source:"                          >> Population.xlsx.eel.yml
$ echo "  url: ../source/Population.xlsx"  >> Population.xlsx.eel.yml
$ eel tree
config
├── LabourForce.xlsx.eel.yml
│   └── LabourForce.xlsx
│       └── Data → memory['WorldBankLabourForce']
└── Population.xlsx.eel.yml
    └── Population.xlsx
        └── Data → memory['WorldBankPopulation']
```

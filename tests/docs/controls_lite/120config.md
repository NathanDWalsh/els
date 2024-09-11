
```{.console #id120config caption="Create a configuration file for the `eel-demo` directory, limiting source tables to `Data`"}
$ pwd
C:\Users\nwals\eel-demo
$ echo "source:"        >  _.eel.yml
$ echo "  table: Data"  >> _.eel.yml
$ eel tree
eel-demo
├── LabourForce.xlsx
│   └── Data → memory['Data']
└── Population.xlsx
    └── Data → memory['Data']
```

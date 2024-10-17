
```{.console #id120config caption="Create a configuration file for the `els-demo` directory, limiting source tables to `Data`"}
$ pwd
C:\Users\nwals\els-demo
$ echo "source:"        >  _.els.yml
$ echo "  table: Data"  >> _.els.yml
$ els tree
els-demo
├── LabourForce.xls
│   └── Data → memory['Data']
└── Population.xls
    └── Data → memory['Data']
```

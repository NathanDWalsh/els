
```{.console #id146root caption="Rename the directory-level `_.eel.yml` to root-level `__.eel.yml`"}
$ ren _.eel.yml __.eel.yml
$ ls -s *.*
C:\Users\nwals\eel-demo\config\world_bank\_.eel.yml
C:\Users\nwals\eel-demo\config\world_bank\LabourForce.xls.eel.yml
C:\Users\nwals\eel-demo\config\world_bank\Population.xls.eel.yml
C:\Users\nwals\eel-demo\config\__.eel.yml
$ eel tree ./world_bank/
config
└── world_bank
    ├── LabourForce.xls.eel.yml
    │   └── LabourForce.xls
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xls.eel.yml
        └── Population.xls
            └── Data → ..\target\WorldBankPopulation.csv
```

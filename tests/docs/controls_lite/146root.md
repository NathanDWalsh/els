
```{.console #id146root caption="Rename the directory-level `_.eel.yml` to root-level `__.eel.yml`"}
$ ren _.eel.yml __.eel.yml
$ ls -s *.*
C:\Users\nwals\eel-demo\config\world_bank\_.eel.yml
C:\Users\nwals\eel-demo\config\world_bank\LabourForce.xlsx.eel.yml
C:\Users\nwals\eel-demo\config\world_bank\Population.xlsx.eel.yml
C:\Users\nwals\eel-demo\config\__.eel.yml
$ eel tree ./world_bank/
config
└── world_bank
    ├── LabourForce.xlsx.eel.yml
    │   └── LabourForce.xlsx
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xlsx.eel.yml
        └── Population.xlsx
            └── Data → ..\target\WorldBankPopulation.csv
```

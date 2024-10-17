
```{.console #id146root caption="Rename the directory-level `_.els.yml` to root-level `__.els.yml`"}
$ ren _.els.yml __.els.yml
$ ls -s *.*
C:\Users\nwals\els-demo\config\world_bank\_.els.yml
C:\Users\nwals\els-demo\config\world_bank\LabourForce.xls.els.yml
C:\Users\nwals\els-demo\config\world_bank\Population.xls.els.yml
C:\Users\nwals\els-demo\config\__.els.yml
$ els tree ./world_bank/
config
└── world_bank
    ├── LabourForce.xls.els.yml
    │   └── LabourForce.xls
    │       └── Data → ..\target\WorldBankLabourForce.csv
    └── Population.xls.els.yml
        └── Population.xls
            └── Data → ..\target\WorldBankPopulation.csv
```

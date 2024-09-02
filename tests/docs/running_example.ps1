cd ~ #rem
Remove-Item -Path ".\eel-demo\" -Recurse -Force > $null 2>&1 #rem
## Remove-Item -Path ".\eel-demo-cp1\" -Recurse -Force > $null 2>&1 #rem
#100new Create a directory and into it download two data files from the World Bank
##eel new eel-demo --yes
mkdir eel-demo
cd eel-demo
$wb_api = "https://api.worldbank.org/v2/indicator/"
$wb_qry = "?downloadformat=excel"
cp D:\Sync\repos\eel\tests\docs\source\API_SP.POP.TOTL_DS2_en_excel_v2_1584408.xls ./Population.xls
cp D:\Sync\repos\eel\tests\docs\source\API_SL.TLF.TOTL.IN_DS2_en_excel_v2_1585272.xls ./LabourForce.xls
## curl -o ./Population.xls ($wb_api + "SP.POP.TOTL" + $wb_qry) -s
## curl -o ./LabourForce.xls ($wb_api + "SL.TLF.TOTL.IN" + $wb_qry) -s
ls 
#108tree Call the `eel tree` command passing in the `Population.xls` file
eel tree Population.xls
#110tree Run the `eel tree` command again, without passing an explicit path
pwd
eel tree
#120config Create a configuration file for the `eel-demo` directory, limiting source tables to `Data`
pwd
echo "source:"        >  _.eel.yml
echo "  table: Data"  >> _.eel.yml
eel tree
#122config Create a source-level config for each source file
echo "target:"                        > LabourForce.xls.eel.yml
echo "  table: WorldBankLabourForce" >> LabourForce.xls.eel.yml
echo "target:"                        > Population.xls.eel.yml
echo "  table: WorldBankPopulation"  >> Population.xls.eel.yml
eel tree
#130source Create source and config directories; and move respective files accordingly
mkdir config
mkdir source
mv *.xls source
mv *.yml config
ls -s *.*
#132source Add a source url to the config files
cd config
echo "source:"                          >> LabourForce.xls.eel.yml
echo "  url: ../source/LabourForce.xls" >> LabourForce.xls.eel.yml
echo "source:"                          >> Population.xls.eel.yml
echo "  url: ../source/Population.xls"  >> Population.xls.eel.yml
eel tree
#140root Create a new sub-directory in the `config` directory called `world_bank` and move the World Bank configs there
mkdir world_bank
mv *.* world_bank
ls -s *.*
#142root Explicitly set a target for the pipeline, using a directory-level config
pwd
echo "target:"                 >  _.eel.yml
echo "  url: ../target/*.csv"  >> _.eel.yml
eel tree
#144root Run `eel tree` on the `world_bank` directory
eel tree ./world_bank/
#146root Rename the directory-level `_.eel.yml` to root-level `__.eel.yml`
ren _.eel.yml __.eel.yml
ls -s *.*
eel tree ./world_bank/
#148multi Create multi-document `world_bank.eel.yml` file.
pwd
## rmdir -r world_bank
echo "target:"                          >  world_bank.eel.yml
echo "  table: WorldBankLabourForce"    >> world_bank.eel.yml
echo "source:"                          >> world_bank.eel.yml
echo "  url: ../source/LabourForce.xls" >> world_bank.eel.yml
echo "  table: Data"                    >> world_bank.eel.yml
echo "---"                              >> world_bank.eel.yml
echo "target:"                          >> world_bank.eel.yml
echo "  table: WorldBankPopulation"     >> world_bank.eel.yml
echo "source:"                          >> world_bank.eel.yml
echo "  url: ../source/Population.xls"  >> world_bank.eel.yml
echo "  table: Data"                    >> world_bank.eel.yml
eel tree world_bank.eel.yml
cd .. #rem
cd .. #rem
## Copy-Item -Path ".\eel-demo\" -Destination ".\eel-demo-cp1\" -Recurse #rem
## cd .\eel-demo\ #rem
cd ~ #rem
## Remove-Item -Path ".\eel-demo\" -Recurse -Force > $null 2>&1 #rem
## Copy-Item -Path ".\eel-demo-cp1\" -Destination ".\eel-demo\" -Recurse #rem
cd .\eel-demo\config\ #rem
#200preview Call `eel preview` command on `Population.xls.eel.yml` to see sample of the `Population.xls` file
cd world_bank
eel preview Population.xls.eel.yml
#210skprows Set the `source.read_excel.skiprows` attribute
echo "  read_excel:"    >> Population.xls.eel.yml
echo "    skiprows: 3"  >> Population.xls.eel.yml
cat Population.xls.eel.yml
eel preview Population.xls.eel.yml
#220transform Add `transform.melt` to config file
echo "transform:"                   >> Population.xls.eel.yml
echo "  melt:"                      >> Population.xls.eel.yml
echo "    id_vars:"                 >> Population.xls.eel.yml
echo "      - Country Name"         >> Population.xls.eel.yml
echo "      - Country Code"         >> Population.xls.eel.yml
echo "      - Indicator Name"       >> Population.xls.eel.yml
echo "      - Indicator Code"       >> Population.xls.eel.yml
echo "    value_name: Population"   >> Population.xls.eel.yml
echo "    var_name: Year"           >> Population.xls.eel.yml
eel preview Population.xls.eel.yml
#225transform Add `transform.astype` to config file
echo "  astype:"                    >> Population.xls.eel.yml
echo "    dtype:"                   >> Population.xls.eel.yml
echo "      Population: Int64"      >> Population.xls.eel.yml
eel preview Population.xls.eel.yml
#235addcols Add new columns.
echo "add_cols:"                       >> Population.xls.eel.yml
echo "  Date Downloaded: 2024-07-16"   >> Population.xls.eel.yml
echo "  Source File: _file_name_full"  >> Population.xls.eel.yml
eel preview Population.xls.eel.yml
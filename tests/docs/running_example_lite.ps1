cd ~ #rem
Remove-Item -Path ".\eel-demo\" -Recurse -Force > $null 2>&1 #rem
## Remove-Item -Path ".\eel-demo-cp1\" -Recurse -Force > $null 2>&1 #rem
#100new Create a directory and into it download two data files from the World Bank
##eel new eel-demo --yes
mkdir eel-demo
cd eel-demo
$wb_api = "https://api.worldbank.org/v2/indicator/"
$wb_qry = "?downloadformat=excel"
cp D:\Sync\repos\eel\tests\docs\source\API_SP.POP.TOTL_DS2_en_excel_v2_1584408_lite.xlsx ./Population.xlsx
cp D:\Sync\repos\eel\tests\docs\source\API_SL.TLF.TOTL.IN_DS2_en_excel_v2_1585272_lite.xlsx ./LabourForce.xlsx
## curl -o ./Population.xlsx ($wb_api + "SP.POP.TOTL" + $wb_qry) -s
## curl -o ./LabourForce.xlsx ($wb_api + "SL.TLF.TOTL.IN" + $wb_qry) -s
ls 
#108tree Call the `eel tree` command passing in the `Population.xlsx` file
eel tree Population.xlsx
#110tree Run the `eel tree` command again, without passing an explicit path
pwd
eel tree
#120config Create a configuration file for the `eel-demo` directory, limiting source tables to `Data`
pwd
echo "source:"        >  _.eel.yml
echo "  table: Data"  >> _.eel.yml
eel tree
#122config Create a source-level config for each source file
echo "target:"                        > LabourForce.xlsx.eel.yml
echo "  table: WorldBankLabourForce" >> LabourForce.xlsx.eel.yml
echo "target:"                        > Population.xlsx.eel.yml
echo "  table: WorldBankPopulation"  >> Population.xlsx.eel.yml
eel tree
#130source Create source and config directories; and move respective files accordingly
mkdir config
mkdir source
mv *.xlsx source
mv *.yml config
ls -s *.*
#132source Add a source url to the config files
cd config
echo "source:"                          >> LabourForce.xlsx.eel.yml
echo "  url: ../source/LabourForce.xlsx" >> LabourForce.xlsx.eel.yml
echo "source:"                          >> Population.xlsx.eel.yml
echo "  url: ../source/Population.xlsx"  >> Population.xlsx.eel.yml
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
echo "  url: ../source/LabourForce.xlsx" >> world_bank.eel.yml
echo "  table: Data"                    >> world_bank.eel.yml
echo "---"                              >> world_bank.eel.yml
echo "target:"                          >> world_bank.eel.yml
echo "  table: WorldBankPopulation"     >> world_bank.eel.yml
echo "source:"                          >> world_bank.eel.yml
echo "  url: ../source/Population.xlsx"  >> world_bank.eel.yml
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
#200preview Call `eel preview` command on `Population.xlsx.eel.yml` to see sample of the `Population.xlsx` file
cd world_bank
eel preview Population.xlsx.eel.yml
#210skprows Set the `source.read_excel.skiprows` attribute
echo "  read_excel:"    >> Population.xlsx.eel.yml
echo "    skiprows: 3"  >> Population.xlsx.eel.yml
cat Population.xlsx.eel.yml
eel preview Population.xlsx.eel.yml
#220transform Add `transform.melt` to config file
echo "transform:"                   >> Population.xlsx.eel.yml
echo "  melt:"                      >> Population.xlsx.eel.yml
echo "    id_vars:"                 >> Population.xlsx.eel.yml
echo "      - Country Name"         >> Population.xlsx.eel.yml
echo "      - Country Code"         >> Population.xlsx.eel.yml
echo "      - Indicator Name"       >> Population.xlsx.eel.yml
echo "      - Indicator Code"       >> Population.xlsx.eel.yml
echo "    value_name: Population"   >> Population.xlsx.eel.yml
echo "    var_name: Year"           >> Population.xlsx.eel.yml
eel preview Population.xlsx.eel.yml
#225transform Add `transform.astype` to config file
echo "  astype:"                    >> Population.xlsx.eel.yml
echo "    dtype:"                   >> Population.xlsx.eel.yml
echo "      Population: Int64"      >> Population.xlsx.eel.yml
eel preview Population.xlsx.eel.yml
#235addcols Add new columns.
echo "add_cols:"                       >> Population.xlsx.eel.yml
echo "  Date Downloaded: 2024-07-16"   >> Population.xlsx.eel.yml
echo "  Source File: _file_name_full"  >> Population.xlsx.eel.yml
eel preview Population.xlsx.eel.yml
cd ~ #rem
Remove-Item -Path ".\els-demo\" -Recurse -Force > $null 2>&1 #rem
## Remove-Item -Path ".\els-demo-cp1\" -Recurse -Force > $null 2>&1 #rem
#100new Create a directory and into it download two data files from the World Bank
##els new els-demo --yes
mkdir els-demo
cd els-demo
$wb_api = "https://api.worldbank.org/v2/indicator/"
$wb_qry = "?downloadformat=excel"
cp D:\Sync\repos\els\tests\docs\source\API_SP.POP.TOTL_DS2_en_excel_v2_1584408_lite.xlsx ./Population.xlsx
cp D:\Sync\repos\els\tests\docs\source\API_SL.TLF.TOTL.IN_DS2_en_excel_v2_1585272_lite.xlsx ./LabourForce.xlsx
## curl -o ./Population.xlsx ($wb_api + "SP.POP.TOTL" + $wb_qry) -s
## curl -o ./LabourForce.xlsx ($wb_api + "SL.TLF.TOTL.IN" + $wb_qry) -s
ls 
#108tree Call the `els tree` command passing in the `Population.xlsx` file
els tree Population.xlsx
#110tree Run the `els tree` command again, without passing an explicit path
pwd
els tree
#120config Create a configuration file for the `els-demo` directory, limiting source tables to `Data`
pwd
echo "source:"        >  _.els.yml
echo "  table: Data"  >> _.els.yml
els tree
#122config Create a source-level config for each source file
echo "target:"                        > LabourForce.xlsx.els.yml
echo "  table: WorldBankLabourForce" >> LabourForce.xlsx.els.yml
echo "target:"                        > Population.xlsx.els.yml
echo "  table: WorldBankPopulation"  >> Population.xlsx.els.yml
els tree
#130source Create source and config directories; and move respective files accordingly
mkdir config
mkdir source
mv *.xlsx source
mv *.yml config
ls -s *.*
#132source Add a source url to the config files
cd config
echo "source:"                          >> LabourForce.xlsx.els.yml
echo "  url: ../source/LabourForce.xlsx" >> LabourForce.xlsx.els.yml
echo "source:"                          >> Population.xlsx.els.yml
echo "  url: ../source/Population.xlsx"  >> Population.xlsx.els.yml
els tree
#140root Create a new sub-directory in the `config` directory called `world_bank` and move the World Bank configs there
mkdir world_bank
mv *.* world_bank
ls -s *.*
#142root Explicitly set a target for the pipeline, using a directory-level config
pwd
echo "target:"                 >  _.els.yml
echo "  url: ../target/*.csv"  >> _.els.yml
els tree
#144root Run `els tree` on the `world_bank` directory
els tree ./world_bank/
#146root Rename the directory-level `_.els.yml` to root-level `__.els.yml`
ren _.els.yml __.els.yml
ls -s *.*
els tree ./world_bank/
#148multi Create multi-document `world_bank.els.yml` file.
pwd
## rmdir -r world_bank
echo "target:"                          >  world_bank.els.yml
echo "  table: WorldBankLabourForce"    >> world_bank.els.yml
echo "source:"                          >> world_bank.els.yml
echo "  url: ../source/LabourForce.xlsx" >> world_bank.els.yml
echo "  table: Data"                    >> world_bank.els.yml
echo "---"                              >> world_bank.els.yml
echo "target:"                          >> world_bank.els.yml
echo "  table: WorldBankPopulation"     >> world_bank.els.yml
echo "source:"                          >> world_bank.els.yml
echo "  url: ../source/Population.xlsx"  >> world_bank.els.yml
echo "  table: Data"                    >> world_bank.els.yml
els tree world_bank.els.yml
cd .. #rem
cd .. #rem
## Copy-Item -Path ".\els-demo\" -Destination ".\els-demo-cp1\" -Recurse #rem
## cd .\els-demo\ #rem
cd ~ #rem
## Remove-Item -Path ".\els-demo\" -Recurse -Force > $null 2>&1 #rem
## Copy-Item -Path ".\els-demo-cp1\" -Destination ".\els-demo\" -Recurse #rem
cd .\els-demo\config\ #rem
#200preview Call `els preview` command on `Population.xlsx.els.yml` to see sample of the `Population.xlsx` file
cd world_bank
els preview Population.xlsx.els.yml
#210skprows Set the `source.read_excel.skiprows` attribute
echo "  read_excel:"    >> Population.xlsx.els.yml
echo "    skiprows: 3"  >> Population.xlsx.els.yml
cat Population.xlsx.els.yml
els preview Population.xlsx.els.yml
#220transform Add `transform.melt` to config file
echo "transform:"                   >> Population.xlsx.els.yml
echo "  melt:"                      >> Population.xlsx.els.yml
echo "    id_vars:"                 >> Population.xlsx.els.yml
echo "      - Country Name"         >> Population.xlsx.els.yml
echo "      - Country Code"         >> Population.xlsx.els.yml
echo "      - Indicator Name"       >> Population.xlsx.els.yml
echo "      - Indicator Code"       >> Population.xlsx.els.yml
echo "    value_name: Population"   >> Population.xlsx.els.yml
echo "    var_name: Year"           >> Population.xlsx.els.yml
els preview Population.xlsx.els.yml
#225transform Add `transform.astype` to config file
echo "  astype:"                    >> Population.xlsx.els.yml
echo "    dtype:"                   >> Population.xlsx.els.yml
echo "      Population: Int64"      >> Population.xlsx.els.yml
els preview Population.xlsx.els.yml
#235addcols Add new columns.
echo "add_cols:"                       >> Population.xlsx.els.yml
echo "  Date Downloaded: 2024-07-16"   >> Population.xlsx.els.yml
echo "  Source File: _file_name_full"  >> Population.xlsx.els.yml
els preview Population.xlsx.els.yml
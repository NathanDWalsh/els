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

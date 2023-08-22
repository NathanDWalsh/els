import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(relativeCreated)d - %(levelname)s - %(message)s')
logging.info('getting started')

logging.info('opening file')
file = pd.ExcelFile('D:\\test_data2\\SalesMX\[Sales] 0685 Dec 22.xlsx')
logging.info('opened file')

print(pd.__version__)


logging.info('reading sheet')
df = pd.read_excel(file,sheet_name='Sales',nrows=100)
logging.info('read sheet')

logging.info('closing file')
file.close()
logging.info('closed file')

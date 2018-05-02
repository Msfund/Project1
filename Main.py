from getdata3 import path,HisDayData
from putdata import HDFutility

a = HisDayData('CFE','IF','20160101','20161231')
dom_data, sub_data = a.GetStitchData()

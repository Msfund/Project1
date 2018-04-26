from getdata3 import path,HisDayData
from putdata import HDFutility

a = HisDayData('CFE','IF','20130101','20131231')
dom_data, sub_data = a.GetStitchData()

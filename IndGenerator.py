import numpy as np
import talib
import ffn
from Indicator_testing import Indicator_testing
from Indicator_setting import *
from HdfUtility import HdfUtility
from dataUlt import EXT_Hdf_Path

# 读入数据
hdf = HdfUtility()
data = hdf.hdfRead(EXT_Hdf_Path,'CFE','IC','Stitch','00','1d',startdate='20110101',enddate='20171231')

# 计算因子
def ma_ind(data):
    for i in MA['period']:
        data['ma'+str(i)] = talib.MA(data['Close'].values,timeperiod=i)
    indname = ["'ma"+str(i)+"'," for i in indparams['ma']['period']]
    return eval("data[["+''.join(indname)+"]]")

ret = ffn.to_returns(data['Close'])
df = ma_ind(data)

# 平稳性检验，有效性检验
Ind_Stability(df)
Ind_Eff(ret,df)


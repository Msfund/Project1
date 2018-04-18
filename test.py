import cx_Oracle
import pandas as pd
import numpy as np
import h5py

# CCommodityFuturesEODPrices：date vt preSettle open high low close volumn openinterest
class GetFutureData:

    def __init__(self,vt,startdate,enddate):
        self.vt = vt
        self.startdate = startdate
        self.enddate = enddate
        db = cx_Oracle.connect('fe','fe','192.168.100.22:1521/winddb')
        self.cursor = db.cursor()

    def define_ExchMarktList(self):
        '''定义不同品种所属列表'''
        exchmarkt = {}
        exchmarkt['CFE'] = 'filesync.CIndexFuturesEODPrices'         # 中金所
        exchmarkt['SHF'] = 'filesync.CCommodityFuturesEODPrices'     # 上期所
        exchmarkt['CZC'] = 'filesync.CCommodityFuturesEODPrices'     # 郑商所
        exchmarkt['DCE'] = 'filesync.CCommodityFuturesEODPrices'     # 大商所
        sql = '''select  s_info_windcode
        from filesync.CFuturesDescription '''"where s_info_windcode LIKE'"+self.vt+'''%' and rownum=1'''
        self.cursor.execute(sql)
        t = self.cursor.fetchall()
        return exchmarkt[t[0][0][-3:]]  #例如'IF1003.CFE'返回的是exchmarkt['CFE']的值

    def get_trade_data(self):
       '''读取日交易数据'''
       sql = '''select s_info_windcode,
       trade_dt,
       s_dq_presettle,
       s_dq_open,
       s_dq_high,
       s_dq_low,
       s_dq_close,
       s_dq_volume,
       s_dq_oi from '''+self.define_ExchMarktList()+'''
       where trade_dt>= '''+self.startdate+' and trade_dt<= '+self.enddate+"and s_info_windcode LIKE'"+self.vt+'''%'
       and LENGTH(s_info_windcode)>=10
       order by trade_dt'''
       self.cursor.execute(sql)
       trade_data = self.cursor.fetchall()
       trade_data = pd.DataFrame(trade_data)
       trade_data.columns = [i[0] for i in self.cursor.description]
       return trade_data

    def get_dom_data(self):

        trade_data = self.get_trade_data()
        trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE','S_DQ_OI'], ascending = [1,1,0], inplace = True)
        # 找出持仓量最大的合约,判断是否为连续三天
        maxoi = trade_data.groupby('TRADE_DT')['S_INFO_WINDCODE'].nth(0)
        lag1_check = maxoi == maxoi.shift(1)
        lag2_check = maxoi == maxoi.shift(2)
        maxoi = pd.DataFrame({'S_INFO_WINDCODE':maxoi, 'check1':lag1_check & lag2_check})
        maxoi = maxoi.reset_index()
        # 判断持仓量最大的合约是否为当月合约
        maxoi['check2'] = maxoi['TRADE_DT'].str[2:6] != maxoi['S_INFO_WINDCODE'].str[2:6]
        
        dom_code = pd.DataFrame({})
        for i in range(len(maxoi)):
            if i == 0:
                dom_code = pd.concat([dom_code, maxoi.loc[maxoi.index[i:i+1],'S_INFO_WINDCODE']],ignore_index=True)
                continue
            dom_code = pd.concat([dom_code, dom_code.iloc[i-1,:]],ignore_index=True) ###copy
            if dom_code.iloc[i,0] != maxoi.iloc[i,1] and maxoi.check1[i] == True and maxoi.check2[i] == True:
                dom_code.iloc[i,0] =  maxoi.iloc[i,1]
        dom_code['1'] = maxoi.TRADE_DT
        dom_code.columns = ['S_INFO_WINDCODE','TRADE_DT']
        dom_data = dom_code.merge(trade_data, on=['TRADE_DT','S_INFO_WINDCODE'], how='left' ) ###
        return dom_data
    

a = GetFutureData('IF','20170101','20171231')
a.get_trade_data()
a.get_dom_data()
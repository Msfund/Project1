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

        trade_data = get_trade_data()
        trade_data.sort_values(by = ['TRADE_DT','S_INFO_CODE','S_DQ_OI'], ascending = [1,1,0], inplace = True)
        # 找出持仓量最大的合约,判断是否为连续三天
        maxvol = trade_data.groupby('TRADE_DT')['S_INFO_CODE'].nth(1)
        maxvol.check1 = maxvol['S_INFO_CODE'] == maxvol['S_INFO_CODE'].shift(1) and \
                           maxvol['S_INFO_CODE'] == maxvol['S_INFO_CODE'].shift(2)
        # 判断持仓量最大的合约是否为当月合约
        maxvol.check2 = maxvol['S_INFO_CODE'].month() != maxvol['TRADE_DT'].month()

        dom_code = pd.DataFrame({})
        for i in range(1,len(trade_data.index)):
            dom_code(i) = maxvol['S_INFO_CODE'](i) if i == 1 else pass
            dom_code(i) = dom_code(i-1) ###copy
            if dom_code(i) != maxvol['S_INFO_CODE'](i) and maxvol.check = True and maxvol.check2 = True:
                dom_code(i) =  maxvol['S_INFO_CODE'](i)

        dom_data = pd.merge(dom_code, trade_data, how='left join', on='S_INFO_CODE') ###
        return dom_data

    def get_subdom_data(self):

        trade_data = get_trade_data()
        # 找出持仓量次大的合约,判断是否为连续三天
        subvol = trade_data.groupby('TRADE_DT')['S_INFO_CODE'].nth(2)
        subvol.check1 = subvol['S_INFO_CODE'] == subvol['S_INFO_CODE'].shift(1) and \
                           subvol['S_INFO_CODE'] == subvol['S_INFO_CODE'].shift(2)

        subdom_code = pd.DataFrame({})
        for i in range(1,len（trade_data.index)):
            subdom_code(i) = subvol['S_INFO_CODE'](i) if i == 1 else pass
            subdom_code(i) = dom_code(i-1) ###copy
            if subdom_code() != subvol['S_INFO_CODE'](i):
                subdom_code(i) = subvol['S_INFO_CODE'](i)

        subdom_data = pd.merge(subdom_code, trade_data, how='left join', on='S_INFO_CODE') ###
        return subdom_data

    def save_data(self):
        f = h5py.File('C:\\users\\user\Desktop\\out.h5','w')
        f.create_dataset('dataset1',data = get_dom_data())
        f.create_dataset('dataset2',data = get_subdom_data())
        f.close()


a = GetFutureData('IF','20170101','20171231')
a.get_trade_data()
# a.get_dom_data()
# a.save.data()

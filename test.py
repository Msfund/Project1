import cx_Oracle
import pandas as pd
import numpy as np
import h5py
import re

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
#------------------------------------------------------------------------------
# 查找退市数据
    def future_delistdate(self):
        # 获取合约退市日期
        sql = '''select fs_info_sccode,
        s_info_windcode,
        s_info_listdate,
        s_info_delistdate
        from filesync.CFuturesDescription
        where s_info_delistdate > '''+self.startdate+" and s_info_windcode LIKE'"+self.vt+'''%'order by s_info_windcode'''
        self.cursor.execute(sql)
        delistdate = pd.DataFrame(self.cursor.fetchall())
        delistdate.columns = [i[0] for i in self.cursor.description]
        return delistdate
#------------------------------------------------------------------------------

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
            dom_code = pd.concat([dom_code, dom_code.iloc[i-1,:]],ignore_index=True)
            if dom_code.iloc[i,0] != maxoi.iloc[i,1] and maxoi.check1[i] == True and maxoi.check2[i] == True:
                dom_code.iloc[i,0] =  maxoi.iloc[i,1]
        dom_code['1'] = maxoi.TRADE_DT
        dom_code.columns = ['S_INFO_WINDCODE','TRADE_DT']
        dom_data = dom_code.merge(trade_data, on=['TRADE_DT','S_INFO_WINDCODE'], how='left' )

        dom_data = self.get_limit_data(dom_data)
        dom_data = self.get_adj_factor(dom_data)
        return dom_data

    def get_sub_data(self):

        trade_data = self.get_trade_data()
        trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE','S_DQ_OI'], ascending = [1,1,0], inplace = True)

        # 找出持仓量次大的合约,判断是否为连续三天
        suboi = trade_data.groupby('TRADE_DT')['S_INFO_WINDCODE'].nth(1)
        lag1_check = suboi == suboi.shift(1)
        lag2_check = suboi == suboi.shift(2)
        suboi = pd.DataFrame({'S_INFO_WINDCODE':suboi, 'check1':lag1_check & lag2_check})
        suboi = suboi.reset_index()

        sub_code = pd.DataFrame({})
        for i in range(len(suboi)):
            if i == 0:
                sub_code = pd.concat([sub_code, suboi.loc[suboi.index[i:i+1],'S_INFO_WINDCODE']],ignore_index=True)
                continue
            sub_code = pd.concat([sub_code, sub_code.iloc[i-1,:]],ignore_index=True)
            if sub_code.iloc[i,0] != suboi.iloc[i,1] and suboi.check1[i] == True:
                sub_code.iloc[i,0] =  suboi.iloc[i,1]
        sub_code['1'] = suboi.TRADE_DT
        sub_code.columns = ['S_INFO_WINDCODE','TRADE_DT']
        sub_data = sub_code.merge(trade_data, on=['TRADE_DT','S_INFO_WINDCODE'], how='left' )

        sub_data = self.get_limit_data(sub_data)
        sub_data = self.get_adj_factor(sub_data)
        return sub_data

    def get_limit_data(self,data):    # 获取 uplimit 和 downlimit
        sql = '''select s_info_windcode,
        s_info_maxpricefluct
        from filesync.CFuturescontpro
        '''"where s_info_windcode LIKE'"+self.vt+'''%'
        order by s_info_windcode'''
        self.cursor.execute(sql)
        limit_info = self.cursor.fetchall()
        limit_info = pd.DataFrame(limit_info)
        limit_info[2] = [int(re.findall(r"\d+\.?\d*",limit_info.iloc[i,1])[0]) for i in range(len(limit_info))]
        del limit_info[1]
        limit_info.columns=['S_INFO_WINDCODE','limit_ratio']
        data = data.merge(limit_info,on='S_INFO_WINDCODE',how='left')
        data['uplimit'] = data['S_DQ_PRESETTLE']*(1+0.01*data['limit_ratio'])
        data['downlimit'] = data['S_DQ_PRESETTLE']*(1-0.01*data['limit_ratio'])
        return data

    def get_adj_factor(self,data):
        # 计算调整因子
        loc = data['S_INFO_WINDCODE'].shift(-1) != data['S_INFO_WINDCODE']
        loc.iloc[-1] = False
        loc = loc.index[loc]
        old = data.ix[loc,]
        new = data.ix[loc+1,]
        new.index = range(len(new))
        old.index = range(len(old))
        code_first = old['S_INFO_WINDCODE'].iloc[0]
        day_first = data['TRADE_DT'].iloc[0]
        date_record =  new['TRADE_DT'].copy()
        date_record.index = range(1,(len(date_record)+1))

        new = new[['S_INFO_WINDCODE','TRADE_DT']]
        new['TRADE_DT'] = old['TRADE_DT']
        new = pd.merge(new,trade_data,how = 'left',on = ['TRADE_DT','S_INFO_WINDCODE'])
        adj_temp = old['S_DQ_CLOSE'] / new['S_DQ_CLOSE']

        adj_temp.index=range(1,(len(adj_temp)+1))
        adj_temp.ix[0]=1.0
        adj_temp = adj_temp.sort_index()
        adj_temp = adj_temp.cumprod()
        new.index=range(1,(len(new)+1))
        new.ix[0]=None
        new['S_INFO_WINDCODE'].ix[0] = code_first
        new['TRADE_DT'].ix[0] = day_first
        new.ix[1:(len(new)-1),['TRADE_DT']] = date_record
        new=new.sort_index()
        new['adj_factor'] = adj_temp
        new = new[['TRADE_DT','S_INFO_WINDCODE','adj_factor']]
        data = pd.merge(new,data,how = 'right',on = ['S_INFO_WINDCODE','TRADE_DT'])
        data = data.sort_values(by = ['TRADE_DT'])
        data['adj_factor'] = data['adj_factor'].fillna(method = 'ffill')
        return data


a = GetFutureData('IF','20170101','20171231')
trade_data = a.get_trade_data()
dom_data = a.get_dom_data()
sub_data = a.get_sub_data()

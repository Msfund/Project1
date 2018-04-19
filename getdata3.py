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
       trade_data = trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE'])
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

#--------------------------------------------------------------------------------
    def get_MarginRatio(self,data):
        ## 获取保证金数据
        sql = '''select
        s_info_windcode,
        marginratio,
        trade_dt
        from filesync.CFuturesmarginratio
        '''"where s_info_windcode LIKE'"+self.vt+'''%'
        order by s_info_windcode'''
        self.cursor.execute(sql)
        Margin_data = pd.DataFrame(self.cursor.fetchall())
        Margin_data.columns = [i[0] for i in self.cursor.description]
        data = data.merge(Margin_data, on = ['S_INFO_WINDCODE','TRADE_DT'],how = 'left')
        data['MARGINRATIO'] = data['MARGINRATIO'].fillna(method = 'ffill')
        if sum(data.isnull()['MARGINRATIO']) != 0:
            #标记未找到对应保证金数据的合约位置
            null_contract = data.isnull()
            list_null = data['S_INFO_WINDCODE'].ix[null_contract['MARGINRATIO']]
            list_uniq = list_null[~list_null.duplicated()]
            #找出一共多有多少品种无对应数据，以及输出品种合约名和其对应保证金数据
            special_Margin = self.special_MarginRatio(list_uniq)
            for i in range(len(special_Margin)):
                data['MARGINRATIO'][list_uniq.index[i]] = special_Margin['MARGINRATIO'][i] #这里只对应品种第一次出现的缺失数据项
            #再次填充
            data['MARGINRATIO'] = data['MARGINRATIO'].fillna(method = 'ffill')
        return data
#------------------------------------------------------------------------------
    def special_MarginRatio(self,list_uniq):
        '''如果合约找不到对应保证金，从CFuturescontpro表中再次查询'''
        '''list_uniq为series格式'''
        list_uniq = str(tuple(list_uniq.tolist()))
        sql='''select s_info_windcode,
        s_info_ftmargins
        from filesync.CFuturescontpro
        where s_info_windcode in'''+list_uniq+'''order by s_info_windcode'''
        self.cursor.execute(sql)
        special_Margin=pd.DataFrame(list(self.cursor.fetchall()))
        special_Margin[2] = [int(re.findall(r"\d+\.?\d*",special_Margin.iloc[i,1])[0]) for i in range(len(special_Margin))]
        del special_Margin[1]
        special_Margin.columns=['S_INFO_WINDCODE','MARGINRATIO']
        return special_Margin

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
#----------------------------------------------------------------------------------
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

    def save_data(self,path):
        dom_data.to_hdf(path,'dom_data')
        sub_data.to_hdf(path,'sub_data')


if __name__  ==  '__main__':
    a = GetFutureData('AU','20170101','20171231')
    trade_data = a.get_trade_data()
    dom_data = a.get_dom_data()
    sub_data = a.get_sub_data()
    # a.save_data('C:\\users\\user\\Desktop\\out1.h5')

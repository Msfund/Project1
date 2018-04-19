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
       trade_data = trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE'])
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
        if len(list_uniq) == 1:
            list_uniq = "('"+list_uniq.tolist()[0]+"')"
        else:
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
#------------------------------------------------------------------------------
    def get_dom_data(self):

        trade_data = self.get_trade_data()
        trade_data.sort_values(by = ['TRADE_DT','S_DQ_OI','S_INFO_WINDCODE'], ascending = [1,0,1], inplace = True)
        maxOI = trade_data.groupby('TRADE_DT').nth(0)
        maxOI = maxOI.reset_index()
        maxOI['S_INFO_WINDCODE1']=None
        #如果为因为切换至下一合约需要3天判断期，主力合约名向后移动3天
        maxOI['S_INFO_WINDCODE'] = maxOI['S_INFO_WINDCODE'].shift(3)
        maxOI['S_INFO_WINDCODE'] = maxOI['S_INFO_WINDCODE'].fillna(method = 'bfill')
        # 确认合约切换点位置
        loca = ~(maxOI['S_INFO_WINDCODE'] == maxOI['S_INFO_WINDCODE'].shift(1))
        # 条件1：持仓量连续第一满3天
        # 找出持仓量最大的合约,判断是否为连续三天，这里check1为标记连续持仓量最大达三天
        check1 = (maxOI['S_INFO_WINDCODE'] == maxOI['S_INFO_WINDCODE'].shift(-1)) & (maxOI['S_INFO_WINDCODE'] == maxOI['S_INFO_WINDCODE'].shift(-2))
        #输入满足条件1的换仓位
        maxOI['S_INFO_WINDCODE1'].ix[(loca)&(check1)] = maxOI['S_INFO_WINDCODE'].ix[(loca)&(check1)]
        #向后填充
        maxOI['S_INFO_WINDCODE1'] = maxOI['S_INFO_WINDCODE1'].fillna(method = 'ffill')
        
        # 条件2：金融期货不会往回切换（比如12月先作为主力合约，那么11月不能成为主力合约）
        # if语句中判断是否往回切换，这里check2为标记出现往回切换的合约
        if self.define_ExchMarktList() == 'filesync.CIndexFuturesEODPrices':
            for i in range(len(maxOI['S_INFO_WINDCODE'])):
                if i == 0:
                    check2 = [False,]
                else:
                    check2.append(maxOI['S_INFO_WINDCODE'][i]<maxOI['S_INFO_WINDCODE'][:i].max())
            check2 = pd.Series(check2)
            #调试满足条件2
            maxOI['S_INFO_WINDCODE1'].ix[check2]=None
            #向前填充，主力合约向后递延
            maxOI['S_INFO_WINDCODE1'] = maxOI['S_INFO_WINDCODE1'].fillna(method = 'bfill')
            
        # 条件3：主力合约切换时不会向当月合约切换
        # 判断持仓量最大的合约是否为当月合约，这里chexk3标记当月合约的主力合约
        temp = (pd.DataFrame([i[2:6] for i in maxOI['S_INFO_WINDCODE']]) == pd.DataFrame([i[-6:-2] for i in maxOI['TRADE_DT']]))
        temp = np.array(temp)
        temp = temp.tolist()
        for i in range(len(temp)):
            if i == 0:
                check3 = [False,]
            else:
                check3.append(temp[i][0])
        check3 = pd.Series(check3)
        #调试满足条件3
        maxOI['S_INFO_WINDCODE1'].ix[loca & check3]=None
        #向前填充，主力合约向后递延
        maxOI['S_INFO_WINDCODE1'] = maxOI['S_INFO_WINDCODE1'].fillna(method = 'bfill')
        #  条件4：如果出现合约退市仍作为主力合约的情况，那么顺延到下一个主力合约
        delistdate = self.future_delistdate()
        maxOI = pd.merge(delistdate,maxOI,how = 'right',on = ['S_INFO_WINDCODE'])
        check4 = (maxOI['TRADE_DT']>=maxOI['S_INFO_DELISTDATE'])
        #调试满足条件4
        maxOI['S_INFO_WINDCODE1'].ix[check4]=None
        #向前填充，主力合约向后递延
        maxOI['S_INFO_WINDCODE1'] = maxOI['S_INFO_WINDCODE1'].fillna(method = 'bfill')
        # 整理数据
        del maxOI['S_INFO_WINDCODE']
        maxOI = maxOI.rename(columns = {'S_INFO_WINDCODE1':'S_INFO_WINDCODE'}) 
        dom_data = pd.merge(maxOI[['S_INFO_WINDCODE','TRADE_DT']],trade_data,how = 'left', on = ['S_INFO_WINDCODE','TRADE_DT'])
        dom_data = self.get_limit_data(dom_data)
        dom_data = self.get_MarginRatio(dom_data)
        dom_data = self.get_adj_factor(dom_data)
        return dom_data
#---------------------------------------------------------------------------
    def get_sub_data(self):

        trade_data = self.get_trade_data()
        trade_data.sort_values(by = ['TRADE_DT','S_DQ_OI','S_INFO_WINDCODE'], ascending = [1,0,1], inplace = True)
        subOI = trade_data.groupby('TRADE_DT').nth(1)
        subOI = subOI.reset_index()
        subOI['S_INFO_WINDCODE2']=None
        #因为切换至下一合约需要3天判断期，主力合约名向后移动3天
        subOI['S_INFO_WINDCODE'] = subOI['S_INFO_WINDCODE'].shift(3)
        subOI['S_INFO_WINDCODE'] = subOI['S_INFO_WINDCODE'].fillna(method = 'bfill')
        # 确认合约切换点位置
        loca = ~(subOI['S_INFO_WINDCODE'] == subOI['S_INFO_WINDCODE'].shift(1))
        # 条件1：持仓量连续第一满3天
        # 找出持仓量最大的合约,判断是否为连续三天，这里check1为标记连续持仓量最大达三天
        check1 = (subOI['S_INFO_WINDCODE'] == subOI['S_INFO_WINDCODE'].shift(-1)) & (subOI['S_INFO_WINDCODE'] == subOI['S_INFO_WINDCODE'].shift(-2))
        #输入满足条件1的换仓位
        subOI['S_INFO_WINDCODE2'].ix[(loca)&(check1)] = subOI['S_INFO_WINDCODE'].ix[(loca)&(check1)]
        #向后填充
        subOI['S_INFO_WINDCODE2'] = subOI['S_INFO_WINDCODE2'].fillna(method = 'ffill')
        # 条件2：主力次主力合约不能为同一张合约
        dom_data = self.get_dom_data()
        dom_data = dom_data.rename(columns = {'S_INFO_WINDCODE':'S_INFO_WINDCODE1'})
        subOI = pd.merge(dom_data[['S_INFO_WINDCODE1','TRADE_DT']],subOI,how = 'right',on = ['TRADE_DT'])
        check2 = subOI['S_INFO_WINDCODE1']==subOI['S_INFO_WINDCODE']
        #调试满足条件2
        subOI['S_INFO_WINDCODE2'].ix[check2]=None
        #向前填充，主力合约向后递延
        subOI['S_INFO_WINDCODE2'] = subOI['S_INFO_WINDCODE2'].fillna(method = 'bfill')
        # # 条件3：如果出现合约退市仍作为主力合约的情况，那么顺延到下一个主力合约
        delistdate = self.future_delistdate()
        subOI = pd.merge(delistdate[['S_INFO_WINDCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE']],subOI,how = 'right',on = ['S_INFO_WINDCODE'])
        check3 = (subOI['TRADE_DT']>=subOI['S_INFO_DELISTDATE'])
        #调试满足条件3
        subOI['S_INFO_WINDCODE2'].ix[check3]=None
        #向前填充，主力合约向后递延
        subOI['S_INFO_WINDCODE2'] = subOI['S_INFO_WINDCODE2'].fillna(method = 'bfill')
# 整理数据 
        del subOI['S_INFO_WINDCODE']
        subOI = subOI.rename(columns = {'S_INFO_WINDCODE2':'S_INFO_WINDCODE'})
        subOI.sort_values(by = ['TRADE_DT'], ascending = [1], inplace = True)
        sub_data = pd.merge(subOI[['S_INFO_WINDCODE','TRADE_DT']],trade_data,how = 'left', on = ['S_INFO_WINDCODE','TRADE_DT'])
        
        sub_data = a.get_MarginRatio(sub_data)
        sub_data = a.get_limit_data(sub_data)
        sub_data = a.get_adj_factor(sub_data)
        return sub_data
#--------------------------------------------------------------------------------
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
#---------------------------------------------------------------------------------------
    def save_data(self,path):
        dom_data.to_hdf(path,'dom_data')
        sub_data.to_hdf(path,'sub_data')
        
#---------------------------------------------------------------------------------------        
if __name__  ==  '__main__':
    a = GetFutureData('IF','20170101','20171231')
    trade_data = a.get_trade_data()
    dom_data = a.get_dom_data()
    sub_data = a.get_sub_data()
    a.save_data('C:\\users\\user\\Desktop\\out1.h5')

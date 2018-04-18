import cx_Oracle
import pandas as pd
import numpy as np
import pymongo
import re
from itertools import groupby
from datetime import timedelta
## the base.py used to defined the constant

# the dom and sub_dom's columns name saved in the csv
DOM_COLUMNS_NAME_csv = {'TRADE_DT1':'date', 'S_INFO_WINDCODE1':'ticker', 'S_DQ_OPEN1':'Open', 'S_DQ_CLOSE1':'Close', 'S_DQ_HIGH1':'High',
                        'S_DQ_LOW1':'Low', 'DownLimit1':'DownLimit','UpLimit1':'UpLimit','S_DQ_PRESETTLE1':'PreSettle','S_DQ_OI1':'OpenInterest',
                    'S_DQ_VOLUME1':'Volume','adj_factor1':'AdjFactor'}
SUB_DOM_DOLUMNS_NAME_csv = {'TRADE_DT2':'date', 'S_INFO_WINDCODE2':'ticker', 'S_DQ_OPEN2':'Open', 'S_DQ_CLOSE2':'Close', 'S_DQ_HIGH2':'High',
                         'S_DQ_LOW2':'Low', 'DownLimit2':'DownLimit','UpLimit2':'UpLimit','S_DQ_PRESETTLE2':'PreSettle','S_DQ_OI2':'OpenInterest',
                        'S_DQ_VOLUME2':'Volume','adj_factor2':'AdjFactor'}
# the dom and sub_dom's colomns name saved in the mongodb

DOM_COLUMNS_NAME_db = {'TRADE_DT1':'datetime','S_INFO_WINDCODE1':'domCode',
                    'S_DQ_CLOSE1':'close','S_DQ_OPEN1':'open','S_DQ_HIGH1':'high','S_DQ_LOW1':'low',
                    'S_DQ_OI1':'oi','S_DQ_PRESETTLE1':'preSettle','S_INFO_PUNIT1':'domMultiplier', 'MARGINRATIO1':'domMarginRatio','limit1':'limit',
                    'adj_factor1':'adjFactor','UpLimit1':'UpLimit','DownLimit1':'DownLimit'}
SUB_DOM_DOLUMNS_NAME_db = {'S_INFO_WINDCODE2':'sub_domCode',
                           'S_DQ_CLOSE2':'sub_close','S_DQ_OPEN2':'sub_open','S_DQ_HIGH2':'sub_high','S_DQ_LOW2':'sub_low',
                            'S_DQ_OI2':'sub_oi','S_DQ_PRESETTLE2':'sub_preSettle','S_INFO_PUNIT2':'sub_domMultiplier', 'MARGINRATIO2':'sub_domMarginRatio',
                            'limit2':'sub_limit','adj_factor2':'sub_adjFactor','UpLimit2':'sub_UpLimit','DownLimit':'sub_DownLimit' }
###### 本模块用于提取和处理数据

def get_data(cursor, startdate, enddate, vtsymbol_names, CSV_FilePath, save_csv = True, insert2db  =  False):
## oracle数据库的游标,数据开始日,数据结束日和需要的品种代码简称,save_csv代表是否将主力和次主力合约数据保存为csv格式,insert2db代表是否插入mongodb,默认不插入
## 将数据按品种进行存储，数据库名为Future_Daily_Db，每个数据库的集合名为品种代码简称
## 其中主力合约和次主力合约的数据存为csv格式
    # 获取品种的基本信息：所有合约以及合约的手数
    contract_info  =  get_contract_info(cursor,vtsymbol_names)
    # 获取所有合约代码
    # 由于oracle的in条件最多为1000，故需要进行or操作
    contract_name  =  list(contract_info['S_INFO_WINDCODE'].unique())
    contract  =  []
    num  =  0
    while(len(contract_name)>1000):
        contract.append(contract_name[0:999])
        contract[num] = str(tuple(contract[num]))
        num += 1
        contract = contract+['or A.s_info_windcode in']
        del contract_name[0:999]
    contract.append(contract_name)
    contract[-1] = str(tuple(contract[-1]))
    contract_name = ''.join(contract)        
    # 获取交易数据
    trade_data = get_trade_dt(cursor,startdate,enddate,contract_name)
    # 获取价格涨跌幅限制
    limit_p_data = get_limit_data(cursor,contract_name)
    # 获取保证金比率限制
    Margin_data = get_Marg(cursor,contract_name)
    # 获取主力/次主力合约代码数据
    dom_data_init = get_domcode(trade_data)
    # 为了之后统一处理，将主力合约代码命名S_INFO_WINDCODE1，次主力为S_INFO_WINDCODE2
    dom_data_init = dom_data_init.rename(columns = {'dom':'S_INFO_WINDCODE1','sub_dom':'S_INFO_WINDCODE2'})
    # 获取主力/次主力合约的相关数据
    dom_data_final = get_domdata(dom_data_init, trade_data, limit_p_data, contract_info, Margin_data,cursor)
    # 将主力/次合约的数据进行存储
    if save_csv:
        save_domdata(CSV_FilePath,dom_data_final)
    # 将数据存入mongodb中
    if insert2db:
        insert2mongodb(dom_data_final,trade_data,vtsymbol_names)
        
def get_contract_info(cursor,vtsymbol_names):
    ## 获取合约基本信息数据
    list_vtnames = []
    list_vtnames.append(vtsymbol_names)
    vtsymbol_names = str(tuple(list_vtnames)).replace(',','')    
    sql = """select s_info_windcode,
    s_info_code,
    s_info_punit
    from filesync.CFuturescontpro
    where s_info_code in""" +vtsymbol_names+"""order by s_info_windcode """
    cursor.execute(sql)
    # 获取合约基本信息
    contract_info1 = cursor.fetchall()
    contract_info1 = pd.DataFrame(contract_info1)
    # 获取列名
    index = cursor.description
    column_name = []
    for name in index:
        column_name.append(name[0])
    contract_info1.columns = column_name
    return contract_info1
    
def get_trade_dt(cursor,startdate,enddate,contract_name):
    ## 获取商品期货的交易日行情
    startdate = str(startdate)
    enddate = str(enddate)      
    sql = '''select 
    A.s_info_windcode,
    B.s_info_code,
    trade_dt,
    s_dq_presettle,
    s_dq_close,
    s_dq_open,
    s_dq_high,
    s_dq_low,
    s_dq_oi,
    s_dq_volume
    from filesync.CCommodityFuturesEODPrices A,filesync.CFuturescontpro B
    where  A.s_info_windcode = B.s_info_windcode and A.s_info_windcode in''' +contract_name+''' and
    trade_dt>= '''+startdate+''' and trade_dt<= '''+enddate+'''order by trade_dt,B.s_info_code'''
    cursor.execute(sql)
    trade_data_Cmd = cursor.fetchall()
    trade_data_Cmd = pd.DataFrame(trade_data_Cmd)
    # 获取列名
    if len(trade_data_Cmd)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        trade_data_Cmd.columns = column_name
    ## 获取股指期货日行情
    sql = '''select 
    A.s_info_windcode,
    B.s_info_code,
    trade_dt,
    s_dq_presettle,
    s_dq_close,
    s_dq_open,
    s_dq_high,
    s_dq_low,
    s_dq_oi,
    s_dq_volume
    from filesync.CIndexFuturesEODPrices A,filesync.CFuturescontpro B
    where  A.s_info_windcode = B.s_info_windcode and A.s_info_windcode in''' +contract_name+''' and
    trade_dt>= '''+startdate+''' and trade_dt<= '''+enddate+'''order by trade_dt,B.s_info_code'''
    cursor.execute(sql)
    trade_data_SIX = cursor.fetchall()
    trade_data_SIX = pd.DataFrame(trade_data_SIX)
    # 获取列名
    if len(trade_data_SIX)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        trade_data_SIX.columns = column_name
    ## 获取国债期货交易数据
    sql = '''select 
    A.s_info_windcode,
    B.s_info_code,
    trade_dt,
    s_dq_presettle,
    s_dq_close,
    s_dq_open,
    s_dq_high,
    s_dq_low,
    s_dq_oi,
    s_dq_volume
    from filesync.CBondFuturesEODPrices A,filesync.CFuturescontpro B
    where  A.s_info_windcode = B.s_info_windcode and A.s_info_windcode in''' +contract_name+''' and
    trade_dt>= '''+startdate+''' and trade_dt<= '''+enddate+'''order by trade_dt,B.s_info_code'''
    cursor.execute(sql)
    trade_data_Treasury = cursor.fetchall()
    trade_data_Treasury = pd.DataFrame(trade_data_Treasury)
    # 获取列名
    if len(trade_data_Treasury)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        trade_data_Treasury.columns = column_name     
    # 合并交易数据
    trade_data_comp = pd.concat([trade_data_Cmd,trade_data_SIX,trade_data_Treasury],axis = 0)
    return trade_data_comp

def get_limit_data(cursor,contract_name):
    ## 获取合约涨跌幅限制数据
    sql = '''select s_info_windcode,
    pct_chg_limit,
    change_dt
    from filesync.CFuturesPriceChangeLimit where s_info_windcode in'''+contract_name+'order by s_info_windcode,change_dt'
    cursor.execute(sql)
    limit_data = cursor.fetchall()
    limit_data = pd.DataFrame(list(limit_data))
    # 获取列名
    if len(limit_data)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        limit_data.columns = column_name  
        return limit_data
def limit_special(code,cursor):
    ## 对于找不到涨跌停板限制的合约，利用正常交易的涨跌停板代替
    sql='''select s_info_windcode,
    s_info_maxpricefluct
    from filesync.CFuturescontpro
    where s_info_windcode ='''+"'"+code+"'"+'''order by s_info_windcode'''
    cursor.execute(sql)
    limit_special_data=pd.DataFrame(list(cursor.fetchall()))
    # 获取列名
    if len(limit_special_data)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        limit_special_data.columns = column_name
    limit_special_data=limit_special_data['S_INFO_MAXPRICEFLUCT'][0]
    limit_special_data=float(re.findall('%*([0-9]*)%',limit_special_data)[0])
    return limit_special_data

def get_Marg(cursor,contract_name):
    ## 获取保证金数据
    sql = '''select 
    s_info_windcode,
    marginratio,
    trade_dt
    from filesync.CFuturesmarginratio
    where s_info_windcode in'''+contract_name+'''order by s_info_windcode''' 
    cursor.execute(sql)
    Margin_data_temp = cursor.fetchall()
    Margin_data_temp = pd.DataFrame(list(Margin_data_temp))
    # 获取列名
    if len(Margin_data_temp)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        Margin_data_temp.columns = column_name  
        return Margin_data_temp    

def Margin_special(code,cursor):
    ## 对于找不到涨跌停板限制的合约，利用正常交易的涨跌停板代替
    sql='''select s_info_windcode,
    s_info_ftmargins
    from filesync.CFuturescontpro
    where s_info_windcode ='''+"'"+code+"'"+'''order by s_info_windcode'''
    cursor.execute(sql)
    Margin_special_data=pd.DataFrame(list(cursor.fetchall()))
    # 获取列名
    if len(Margin_special_data)!= 0:
        column_name = []
        index = cursor.description
        for name in index:
            column_name.append(name[0])
        Margin_special_data.columns = column_name
    Margin_special_data=Margin_special_data['S_INFO_FTMARGINS'][0]
    Margin_special_data=float(re.findall('%*([0-9]*)%',Margin_special_data)[0])
    return Margin_special_data

def get_domcode(trade_da):
    ### 该函数用于返回主力/次主力合约的代码
    ### 输入参数为品种的日交易数据
        # 获得每天持仓量最大的合约，和持仓量次大的合约
        trade_da = trade_da.sort_values(by = ['TRADE_DT','S_INFO_CODE','S_DQ_OI'],ascending = [1,1,0]) 
        dom_data = trade_da.groupby([trade_da['TRADE_DT'],trade_da['S_INFO_CODE']]).head(1)
        sub_dom_data = trade_da.groupby([trade_da['TRADE_DT'],trade_da['S_INFO_CODE']]).head(2)
        sub_dom_data = sub_dom_data.sort_values(by = ['TRADE_DT','S_DQ_OI'],ascending = [1,1])
        sub_dom_data = sub_dom_data[~sub_dom_data['TRADE_DT'].duplicated()]
        dom_data.index = range(len(dom_data['TRADE_DT']))
        sub_dom_data.index = range(len(sub_dom_data['TRADE_DT']))
        columns_new = dom_data.columns+str(1)
        dom_data.columns = columns_new
        columns_new = sub_dom_data.columns+str(2)
        sub_dom_data.columns = columns_new
        data_comp = pd.concat([dom_data,sub_dom_data],axis = 1)
        
        # 求出连续成为最大交易量的合约和次数
        for i in range(2):
            data_comp['count'+str(i+1)] = None
            code = [list(g) for a,g in groupby(data_comp['S_INFO_WINDCODE'+str(i+1)])]
            count = pd.Series([len(list(g)) for a,g in groupby(data_comp['S_INFO_WINDCODE'+str(i+1)])])
            # 将个数对应到数据中
            loc1 = data_comp['S_INFO_WINDCODE'+str(i+1)].shift(-1) == data_comp['S_INFO_WINDCODE'+str(i+1)]
            data_comp['count'+str(i+1)].ix[~loc1] = list(count)
            data_comp['count'+str(i+1)] = data_comp['count'+str(i+1)].fillna(method = 'bfill')
            # 剔出掉连续持仓量小于3天的合约数据(除了第一份合约以外，因为第一份合约不需要满足连续三天)
            loc2 = data_comp['count'+str(i+1)]<3
            loc3 =  data_comp['S_INFO_WINDCODE'+str(i+1)] ==  data_comp['S_INFO_WINDCODE'+str(i+1)][0]
            data_comp['S_INFO_WINDCODE'+str(i+1)].ix[loc2&(~loc3)] = None
            data_comp['S_INFO_WINDCODE'+str(i+1)] = data_comp['S_INFO_WINDCODE'+str(i+1)].fillna(method = 'ffill')
            # 满足三天的条件
            data_comp['S_INFO_WINDCODE'+str(i+1)] = data_comp['S_INFO_WINDCODE'+str(i+1)].shift(3)
            data_comp['S_INFO_WINDCODE'+str(i+1)] = data_comp['S_INFO_WINDCODE'+str(i+1)].fillna(method = 'bfill')
        data_comp_init = data_comp
        
        # 由于计算主力/次主力合约时有三天的延后时间导致合约退市但仍为主力/次主力合约
        # 所以这里简单处理，如果合约退市，那么下一个主力合约将提前成为主力/次主力合约
        # 但是如果是最后一天的话，那么将对第一个条件所导致的这种情况选择不满3天也可以作为主力/次主力合约
        for i in range(2):
            data_comp_temp = pd.merge(data_comp_init,trade_da,how = 'left',left_on = ['S_INFO_WINDCODE'+str(i+1),'TRADE_DT'+str(i+1)],right_on = ['S_INFO_WINDCODE','TRADE_DT'])
            # 找到退市的合约位置
            loc = data_comp_temp['S_DQ_CLOSE'].astype(str) == 'nan'
            data_comp_init['S_INFO_WINDCODE'+str(i+1)].ix[loc] = None
            data_comp_init['S_INFO_WINDCODE'+str(i+1)] = data_comp_init['S_INFO_WINDCODE'+str(i+1)].fillna(method = 'bfill')
            loc1_index = data_comp_init.index[data_comp_init['S_INFO_WINDCODE'+str(i+1)].astype(str) =='None']
            if i==0:
                data_comp_init['S_INFO_WINDCODE'+str(i+1)].ix[loc1_index] = dom_data['S_INFO_WINDCODE'+str(i+1)].ix[loc1_index]
            if i==1:
                data_comp_init['S_INFO_WINDCODE'+str(i+1)].ix[loc1_index] = sub_dom_data['S_INFO_WINDCODE'+str(i+1)].ix[loc1_index]
        
        # 如果是金融期货,那么向前切换,也就是逆序将得到保持
        # 获取交易年月,和交易所名称,用于向前切换和之后的第三个条件
        data_comp_init['Exchmarket'] = re.findall('\.([A-Z]*)%*',data_comp_init['S_INFO_WINDCODE1'][0])[0]
        for i in range(2):
            date_temp = data_comp_init['S_INFO_WINDCODE'+str(i+1)].str.replace('([A-Z.])','')
            # 这里主要防止某些合约出现TA801只给出3位数字的情况
            # 还有出现0918也就是200918导致转换为浮点型数据失败
            for j in range(len(date_temp)):
                lens = len(date_temp[j])
                if lens  == 3:
                    date_temp[j] = '1'+date_temp[j]
            for j in range(len(date_temp)):
                if date_temp[j][0] == '0':
                    date_temp[j] = date_temp[j][1::]
            data_comp_init['contract_date'+str(i+1)] = date_temp
            data_comp_init['contract_date'+str(i+1)] = data_comp_init['contract_date'+str(i+1)].astype(float)
            
            data_comp_further = data_comp_init.copy()   
            
           # 金融期货(除国债期货以外)向前切换
            if (data_comp_init['Exchmarket'][0] == 'CFE') & (data_comp_init['S_INFO_CODE1'][0].find('T') == -1):
                temp = data_comp_init.copy()                
                for i in range(2):
                    loc = temp['S_INFO_WINDCODE'+str(i+1)].duplicated()
                    f_code = temp['S_INFO_WINDCODE'+str(i+1)][~loc]
                    for j in range(len(f_code)):
                        # 找到逆序
                        code = f_code.iloc[j]
                        try:
                            loc1 = temp[temp['S_INFO_WINDCODE'+str(i+1)] == code]['contract_date'+str(i+1)].iloc[0]>temp['contract_date'+str(i+1)]
                        except:
                            continue
                        loc2 = temp['S_INFO_WINDCODE'+str(i+1)].index>f_code.index[j]
                        loc3 = loc1&loc2
                        if sum(loc3)!= 0:
                            temp['S_INFO_WINDCODE'+str(i+1)][loc3] = code
                            temp['contract_date'+str(i+1)][loc3] = temp[temp['S_INFO_WINDCODE'+str(i+1)] == code]['contract_date'+str(i+1)].iloc[0]
                data_comp_further = temp


            
        # 最后一个条件，主力/次主力合约不会向当月合约切换
        # 如果出现这种情况主力/次主力向后顺延
        for i in range(2):
            date_temp = data_comp_further['TRADE_DT'+str(i+1)].astype(str).str[2:6]
            # 为了转化为浮点型进行比较需要对0918这种0开头的进行处理，把它变为918
            loc1 = date_temp.str[0] == '0'
            date_temp[loc1] = date_temp[loc1].str.lstrip('0')
            data_comp_further['date_num'+str(i+1)] = date_temp.astype(float)
            # 找到切换点
            loc2 = data_comp_further['S_INFO_WINDCODE'+str(i+1)].shift(1) == data_comp_further['S_INFO_WINDCODE'+str(i+1)]
            loc2[0] = True
            # 找出主力合约作为当月合约的位置
            loc3 = data_comp_further['date_num'+str(i+1)] == data_comp_further['contract_date'+str(i+1)]
            # 找到向当月合约切换的合约,也就是同时满足上面两个条件
            loc4 = loc3&(~loc2)
            if sum(loc4) == 0:
                continue
            f_code = data_comp_further['S_INFO_WINDCODE'+str(i+1)][loc4]
            for saf_code in f_code:
                loc5 = data_comp_further['S_INFO_WINDCODE'+str(i+1)] == saf_code
                data_comp_further['S_INFO_WINDCODE'+str(i+1)][loc5] = None
            # 最后利用向后填补的方法得到主力/次主力合约
            data_comp_further['S_INFO_WINDCODE'+str(i+1)] = data_comp_further['S_INFO_WINDCODE'+str(i+1)].fillna(method = 'bfill')
        # 利用向后填补解决次主力合约和主力合约同时为一张合约的情况
        loc6 = data_comp_further['S_INFO_WINDCODE1'] == data_comp_further['S_INFO_WINDCODE2']
        data_comp_further['S_INFO_WINDCODE2'][loc6] = None
        data_comp_further['S_INFO_WINDCODE2'] = data_comp_further['S_INFO_WINDCODE2'].fillna(method = 'bfill')             
        data_comp_final = data_comp_further.ix[:,['TRADE_DT1','S_INFO_WINDCODE1','S_INFO_WINDCODE2']]     
        data_comp_final = data_comp_final.rename(columns = {'TRADE_DT1':'TRADE_DT','S_INFO_WINDCODE':'dom','S_INFO_WINDCODE2':'sub_dom'})
        return data_comp_final

def get_domdata(dom_code,trade_da,limit_p_data,contract_info,Margin_data,cursor):
    ## 获取需要的主力和次主力合约的相关数据
        # 合并日交易数据
        dom_data_temp = dom_code.copy().iloc[:,0]
        for i in range(2):
            dom_data = pd.merge(trade_da,dom_code,how = 'right',left_on = ['S_INFO_WINDCODE','TRADE_DT'],right_on = ['S_INFO_WINDCODE'+str(i+1),'TRADE_DT'])
            dom_data = dom_data.drop(['S_INFO_WINDCODE1','S_INFO_WINDCODE2'],axis = 1)
            dom_data.columns = dom_data.columns+str(i+1)
            dom_data_temp = pd.concat([dom_data_temp,dom_data],axis = 1)
        dom_code = dom_data_temp.iloc[:,1:len(dom_data_temp)]
        # 合并涨跌停限制
        limit_p_data['CHANGE_DT'] = pd.to_datetime(limit_p_data['CHANGE_DT'])
        dom_data_temp = dom_code.copy()
        for i in range(2):
            dom_data_temp['TRADE_DT'+str(i+1)] = pd.to_datetime(dom_data_temp['TRADE_DT'+str(i+1)].astype(str))
            dom_data_temp['limit'+str(i+1)] = None
            f_code = pd.unique(dom_data_temp['S_INFO_WINDCODE'+str(i+1)])
            limit_temp1 = pd.Series([0])
            for code in f_code:
                # 可能会出现有的合约没有涨跌停的变动的数据，那调用limit_special函数求出正常情况下该合约的涨跌停板限制
                loc = limit_p_data['CHANGE_DT'][limit_p_data['S_INFO_WINDCODE'] == code]<= dom_data_temp['TRADE_DT'+str(i+1)][dom_data_temp['S_INFO_WINDCODE'+str(i+1)] == code].iloc[0]
                if sum(loc)==0:
                    limit=limit_special(code,cursor)
                else:
                    limit = limit_p_data['PCT_CHG_LIMIT'][limit_p_data['S_INFO_WINDCODE'] == code][loc].iloc[sum(loc)-1]
                first_day = dom_data_temp['limit'+str(i+1)][dom_data_temp['S_INFO_WINDCODE'+str(i+1)] == code].index[0]
                limit_temp2 = dom_data_temp['limit'+str(i+1)].ix[dom_data_temp['S_INFO_WINDCODE'+str(i+1)] == code,]
                limit_temp2.ix[first_day] = limit
                limit_temp1 = pd.concat([limit_temp1,limit_temp2])
            dom_data_temp['limit'+str(i+1)] = limit_temp1.iloc[1:len(limit_temp1),]
            dom_data_temp1 = pd.merge(dom_data_temp,limit_p_data,how = 'left',left_on = ['S_INFO_WINDCODE'+str(i+1),'TRADE_DT'+str(i+1)],right_on = ['S_INFO_WINDCODE','CHANGE_DT'])
            loc = dom_data_temp1['limit'+str(i+1)]!= dom_data_temp1['PCT_CHG_LIMIT']
            dom_data_temp1['limit'+str(i+1)][loc] = dom_data_temp1['limit'+str(i+1)][loc].fillna(0)+dom_data_temp1['PCT_CHG_LIMIT'][loc].fillna(0)
            dom_data_temp1['limit'+str(i+1)][dom_data_temp1['limit'+str(i+1)] == 0] = None
            dom_data_temp1['limit'+str(i+1)] = dom_data_temp1['limit'+str(i+1)].fillna(method = 'ffill')
            # 计算今日的涨跌停额度
            dom_data_temp1['UpLimit'+str(i+1)] = (dom_data_temp1['S_DQ_PRESETTLE'+str(i+1)])*(1+dom_data_temp1['limit'+str(i+1)]/100)
            dom_data_temp1['DownLimit'+str(i+1)] = (dom_data_temp1['S_DQ_PRESETTLE'+str(i+1)])*(1-dom_data_temp1['limit'+str(i+1)]/100)
            loc1 = pd.Series((dom_data_temp1.columns.str[-1]!= '1')&(dom_data_temp1.columns.str[-1]!= '2'))
            loc2 = loc1.index[~loc1]
            # 去掉不需要的列
            dom_data_temp = dom_data_temp1.iloc[:,loc2]
        dom_data_further = dom_data_temp.copy()
        
        # 将合约基本数据合并
        for i in range(2):
            contract_info = contract_info.rename(columns = {'S_INFO_WINDCODE':'S_INFO_WINDCODE'+str(i+1)})
            dom_data_further = pd.merge(dom_data_further,contract_info,how = 'left',on = 'S_INFO_WINDCODE'+str(i+1))
            dom_data_further = dom_data_further.drop(['S_INFO_CODE'],axis = 1)
            dom_data_further = dom_data_further.rename(columns = {'S_INFO_PUNIT':'S_INFO_PUNIT'+str(i+1)})
            contract_info = contract_info.rename(columns = {'S_INFO_WINDCODE'+str(i+1):'S_INFO_WINDCODE'})
        # 将保证金数据合并
        Margin_data['TRADE_DT'] = pd.to_datetime(Margin_data['TRADE_DT'])
        Margin_data = Margin_data.sort_values(by = ['S_INFO_WINDCODE','TRADE_DT'],ascending = [1,1])
        Margin_data['MARGINRATIO'] = Margin_data['MARGINRATIO'].astype(float)/100.0
        dom_data_temp = dom_data_further.copy()
        for i in range(2):
            dom_data_temp['TRADE_DT'+str(i+1)] = pd.to_datetime(dom_data_temp['TRADE_DT'+str(i+1)])
            dom_data_temp['MARGINRATIO'+str(i+1)] = None
            f_code = pd.unique(dom_data_temp['S_INFO_WINDCODE'+str(i+1)])
            Margin_temp1 = pd.Series([0])
            for code in f_code:
                loc = Margin_data['TRADE_DT'][Margin_data['S_INFO_WINDCODE'] == code]<= dom_data_temp['TRADE_DT'+str(i+1)][dom_data_temp['S_INFO_WINDCODE'+str(i+1)] == code].iloc[0]
                if sum(loc)==0:
                    Margin=Margin_special(code, cursor)
                else:
                    Margin = Margin_data['MARGINRATIO'][Margin_data['S_INFO_WINDCODE'] == code][loc].iloc[sum(loc)-1]
                first_day = dom_data_temp['MARGINRATIO'+str(i+1)][dom_data_temp['S_INFO_WINDCODE'+str(i+1)] == code].index[0]
                Margin_temp2 = dom_data_temp['MARGINRATIO'+str(i+1)].ix[dom_data_temp['S_INFO_WINDCODE'+str(i+1)] == code,]
                Margin_temp2.ix[first_day] = Margin
                Margin_temp1 = pd.concat([Margin_temp1,Margin_temp2])
            dom_data_temp['MARGINRATIO'+str(i+1)] = Margin_temp1.iloc[1:len(Margin_temp1),]
            dom_data_temp1 = pd.merge(dom_data_temp,Margin_data,how = 'left',left_on = ['S_INFO_WINDCODE'+str(i+1),'TRADE_DT'+str(i+1)],right_on = ['S_INFO_WINDCODE','TRADE_DT'])
            loc = dom_data_temp1['MARGINRATIO'+str(i+1)]!= dom_data_temp1['MARGINRATIO']
            dom_data_temp1['MARGINRATIO'+str(i+1)][loc] = dom_data_temp1['MARGINRATIO'+str(i+1)][loc].fillna(0)+dom_data_temp1['MARGINRATIO'][loc].fillna(0)
            dom_data_temp1['MARGINRATIO'+str(i+1)][dom_data_temp1['MARGINRATIO'+str(i+1)] == 0] = None
            dom_data_temp1['MARGINRATIO'+str(i+1)] = dom_data_temp1['MARGINRATIO'+str(i+1)].fillna(method = 'ffill')
            dom_data_temp1 = dom_data_temp1.drop(['MARGINRATIO','TRADE_DT','S_INFO_WINDCODE'],axis = 1)
            dom_data_temp = dom_data_temp1
            
        dom_data_further = dom_data_temp.copy()        

        # 计算调整因子
        trade_da['TRADE_DT'] = pd.to_datetime(trade_da['TRADE_DT'].astype(str))
        for i in range(2):
            # 找到切换点
            loc = dom_data_further['S_INFO_WINDCODE'+str(i+1)].shift(-1)!= dom_data_further['S_INFO_WINDCODE'+str(i+1)]
            loc.iloc[len(loc)-1] = False
            loc = loc.index[loc]
            old = dom_data_further.ix[loc,]
            new = dom_data_further.ix[loc+1,]
            new.index = range(len(new))
            old.index = range(len(old))
            code_first = old['S_INFO_WINDCODE'+str(i+1)].iloc[0]
            day_first = dom_data_further['TRADE_DT'+str(i+1)].iloc[0]
            date_record =  new['TRADE_DT'+str(i+1)].copy()
            date_record.index = range(1,(len(date_record)+1))
            new['TRADE_DT'+str(i+1)] = old['TRADE_DT'+str(i+1)]
            new = new[['TRADE_DT'+str(i+1),'S_INFO_WINDCODE'+str(i+1)]]
            new = pd.merge(new,trade_da,how = 'left',left_on = ['TRADE_DT'+str(i+1),'S_INFO_WINDCODE'+str(i+1)],right_on = ['TRADE_DT','S_INFO_WINDCODE'])
            # 这里用的是主力合约最后一天的收盘价相除
            adj_temp = old['S_DQ_CLOSE'+str(i+1)][0:len(old)]/new['S_DQ_CLOSE']
            # 第一天的合约的调整因子为1
            adj_temp.index=range(1,(len(adj_temp)+1))
            adj_temp.ix[0]=1.0
            adj_temp = adj_temp.sort_index()
            adj_temp = adj_temp.cumprod()
            new.index=range(1,(len(new)+1))
            new.ix[0]=None
            new['S_INFO_WINDCODE'+str(i+1)].ix[0] = code_first
            new['TRADE_DT'+str(i+1)].ix[0] = day_first
            new.ix[1:(len(new)-1),['TRADE_DT'+str(i+1)]] = date_record
            new=new.sort_index()
            new['adj_factor'+str(i+1)] = adj_temp
            new = new[['TRADE_DT'+str(i+1),'S_INFO_WINDCODE'+str(i+1),'adj_factor'+str(i+1)]]
            dom_data_further = pd.merge(new,dom_data_further,how = 'right',on = ['S_INFO_WINDCODE'+str(i+1),'TRADE_DT'+str(i+1)])
            dom_data_further = dom_data_further.sort_values(by = ['TRADE_DT'+str(i+1)])
            dom_data_further['adj_factor'+str(i+1)] = dom_data_further['adj_factor'+str(i+1)].fillna(method = 'ffill')
        return dom_data_further
        
    

def save_domdata(CSV_FilePath,dom_data):
    ## 将主力次主力合约数据存成csv格式
    vtsymbol_names = dom_data['S_INFO_CODE1'][0]
    Exchmarket = re.findall('\.([A-Z]*)%*',dom_data['S_INFO_WINDCODE1'][0])[0]
    for i in range(2):
        data_temp = dom_data.iloc[:,dom_data.columns.str[-1] == str(i+1)]
        if i == 0:
            data_temp = data_temp[['TRADE_DT1','S_INFO_WINDCODE1', 'S_DQ_OPEN1', 'S_DQ_CLOSE1', 'S_DQ_LOW1', 'S_DQ_HIGH1','S_DQ_PRESETTLE1',
                                    'DownLimit1', 'UpLimit1', 'S_DQ_VOLUME1',  'S_DQ_OI1',  'adj_factor1']]                   
            data_temp = data_temp.rename(columns = DOM_COLUMNS_NAME_csv)
            data_temp.to_csv(CSV_FilePath+Exchmarket+'_'+vtsymbol_names+'_dom.csv',index = False)
            print '%s主力合约保存成功'%vtsymbol_names
        else:
            data_temp = data_temp[['TRADE_DT2','S_INFO_WINDCODE2', 'S_DQ_OPEN2', 'S_DQ_CLOSE2', 'S_DQ_LOW2', 'S_DQ_HIGH2','S_DQ_PRESETTLE2',
                                    'DownLimit2', 'UpLimit2', 'S_DQ_VOLUME2',  'S_DQ_OI2',  'adj_factor2']]      
            data_temp = data_temp.rename(columns = SUB_DOM_DOLUMNS_NAME_csv)
            data_temp.to_csv(CSV_FilePath+Exchmarket+'_'+vtsymbol_names+'_sub_dom.csv',index = False)
            print '%s次主力合约保存成功'%vtsymbol_names     
 
def insert2mongodb(dom_data_final, trade_data, vtsymbol_names):
    ## 将主力/次主力合约的数据和交易数据插入mongodb中
    ## 将数据按品种进行存储    
    client  =  pymongo.MongoClient('localhost', 27017)
    
    dom_data_final.index = dom_data_final['TRADE_DT1'].astype(str)
    data_final = dom_data_final.copy()
    trdata = trade_data.copy()
    for ct in pd.unique(trdata['S_INFO_WINDCODE']):
        trdata_temp = trdata[trdata['S_INFO_WINDCODE'] == ct]
        trdata_temp.index = trdata_temp['TRADE_DT'].astype(str)
        ct = ct.split('.')[0]
        trdata_temp = trdata_temp.rename(columns = {'S_DQ_CLOSE':ct+'_close','S_DQ_OPEN':ct+'_open','S_DQ_HIGH':ct+'_high','S_DQ_LOW':ct+'_low','S_DQ_OI':ct+'_pos','S_DQ_PRESETTLE':ct+'_preSettle'})
        trdata_temp = trdata_temp[[ct+'_close',ct+'_open',ct+'_high',ct+'_low',ct+'_pos',ct+'_preSettle']]
        #   将收盘价这一列的空值按照向前填补方式补齐数据
        data_final = pd.concat([data_final,trdata_temp],axis = 1)
        data_final[ct+'_close'] = data_final[ct+'_close'].fillna(method = 'ffill')
    # 更改主力和次主力合约的列名
    data_final = data_final.rename(columns = DOM_COLUMNS_NAME_db)
    
    data_final = data_final.rename(columns = SUB_DOM_DOLUMNS_NAME_db)
    data_final['vtSymbol']  =  data_final['domCode']
    # 删除不需要的列
    data_final = data_final.drop('TRADE_DT2',axis = 1)
    data_final  =  data_final.reset_index(drop  =  True)        
    # 插入数据
    client['Future_Daily_Db'][vtsymbol_names].insert_many(data_final.T.to_dict().values())
    print u'%s插入mongodb成功'%vtsymbol_names
    
def future_Calendar(startdate,enddate,cursor):
    sql = '''select 
    Trade_days,
    S_info_exchmarket
    from filesync.CFuturesCalendar
    where Trade_days>= '''+startdate+'''and Trade_days<= '''+enddate+''' and S_info_exchmarket = 'CFFEX'
    order by Trade_days'''
    cursor.execute(sql)
    date = pd.DataFrame(cursor.fetchall())
    # 获取列名
    column_name = []
    index = cursor.description
    for name in index:
        column_name.append(name[0])
    date.columns = column_name
    # 时间格式转化
    date['TRADE_DAYS'] = pd.to_datetime(date['TRADE_DAYS'])
    date = date['tradingDay'] = date.rename(columns={'TRADE_DAYS':'tradingDay'})
    # 插入时间
    client  =  pymongo.MongoClient('localhost', 27017)    
    client['Future_Daily_Db']['Future_Calendar'].insert_many(date.T.to_dict().values())
    print '交易日期插入成功'
    
def future_delistdate(startdate, client, cursor, vtSymbol):
    # 获取合约退市日期
    symbol = vtSymbol
    vtSymbol = "'"+vtSymbol+"'"
    sql = '''select fs_info_sccode,
    s_info_windcode,
    s_info_listdate,
    s_info_delistdate
    from filesync.CFuturesDescription
    where s_info_delistdate > '''+startdate+''' and fs_info_sccode = '''+vtSymbol+''' order by s_info_windcode'''
    cursor.execute(sql)
    delistdate = pd.DataFrame(cursor.fetchall())
    # 获取列名
    column_name = []
    index = cursor.description
    for name in index:
        column_name.append(name[0])
    delistdate.columns = column_name
    # 时间格式转换
    delistdate['S_INFO_LISTDATE'] = pd.to_datetime(delistdate['S_INFO_LISTDATE'])
    delistdate['S_INFO_DELISTDATE'] = pd.to_datetime(delistdate['S_INFO_DELISTDATE'])
    delistdate.rename(columns = {'S_INFO_LISTDATE':'listDate','S_INFO_DELISTDATE':'delistDate',
                                 'FS_INFO_SCCODE':'Code','S_INFO_WINDCODE':'vtSymbol'}, inplace = True)
    # 插入数据
    client['Future_Delistdate'][symbol].insert_many(delistdate.T.to_dict().values())
    print u'%s退市时间插入成功'%symbol    
    

def future_tradingAmount(startdate, client, cursor, vtSymbol):
    # 获取商品期货的交易额
    contractName = future_contractName(startdate, cursor, vtSymbol)
    sql = """select distinct fs_info_sccode as Code,
    trade_dt as datetime,
    sum(s_dq_volume)over(partition by trade_dt,fs_info_sccode) as volume,
    sum(s_dq_amount)over(partition by trade_dt,fs_info_sccode) as amount
    from
    (select fs_info_sccode,
    B.s_info_windcode,
    trade_dt,
    s_dq_volume,
    s_dq_amount
    from filesync.CCommodityFuturesEODPrices B, filesync.CFuturesDescription A
    where A.s_info_windcode = B.s_info_windcode
    and trade_dt >= """+startdate+""" and B.fs_info_type = 2 
    and A.s_info_windcode in """+contractName+""" order by trade_dt, B.s_info_windcode)
    order by datetime, code"""
    cursor.execute(sql)
    tradingAmount = pd.DataFrame(cursor.fetchall())
    # 获取列名
    column_name = []
    index = cursor.description
    for name in index:
        column_name.append(name[0])
    tradingAmount.columns = column_name
    
    # 时间格式转换
    tradingAmount.rename(columns = {'DATETIME':'datetime','CODE':'Code',
                                 'VOLUME':'volume','AMOUNT':'amount'}, inplace = True)
    # 插入数据
    client['Future_tradingAmount']['tradingAmount'].insert_many(tradingAmount.T.to_dict().values())
    print u'%s交易额数据插入成功'%vtSymbol


def ffuture_tradingAmount(startdate, client, cursor, vtSymbol):
    # 获取金融期货的交易量
    vtSymbol = "'"+vtSymbol+"'"
    sql = """select distinct fs_info_sccode as Code,
    trade_dt as datetime,
    sum(s_dq_volume)over(partition by trade_dt,fs_info_sccode) as volume,
    sum(s_dq_amount)over(partition by trade_dt,fs_info_sccode) as amount
    from
    (select fs_info_sccode,
    B.s_info_windcode,
    trade_dt,
    s_dq_volume,
    s_dq_amount
    from filesync.CIndexFuturesEODPrices B, filesync.CFuturesDescription A
    where A.s_info_windcode = B.s_info_windcode
    and trade_dt >= """+startdate+""" and B.fs_info_type = 2 
    and A.fs_info_sccode = """+vtSymbol+""" order by trade_dt, B.s_info_windcode)
    order by datetime, code"""
    cursor.execute(sql)
    tradingAmount = pd.DataFrame(cursor.fetchall())
    # 获取列名
    column_name = []
    index = cursor.description
    for name in index:
        column_name.append(name[0])
    tradingAmount.columns = column_name
    
    # 时间格式转换
    tradingAmount.rename(columns = {'DATETIME':'datetime','CODE':'Code',
                                 'VOLUME':'volume','AMOUNT':'amount'}, inplace = True)
    # 插入数据
    client['Future_tradingAmount']['tradingAmount'].insert_many(tradingAmount.T.to_dict().values())
    print u'%s交易额数据插入成功'%vtSymbol

def future_contractName(startdate, cursor, vtSymbol):
    vtSymbol = "'"+vtSymbol+"'"
    #vtSymbol = str(tuple(vtSymbol))
    sql = """select s_info_windcode,
    fs_info_sccode
    from filesync.CFuturesDescription
    where s_info_delistdate >= """+startdate+"""and fs_info_sccode = """+vtSymbol+"""order by fs_info_sccode, s_info_windcode"""
    cursor.execute(sql)
    contractInfo = pd.DataFrame(cursor.fetchall())
    # 获取列名
    column_name = []
    index = cursor.description
    for name in index:
        column_name.append(name[0])
    contractInfo.columns = column_name
    
    # sql取数，条件不得超过1000条，在此用or进行连接
    contract_name  =  list(contractInfo['S_INFO_WINDCODE'].unique())
    contractName = str(tuple(contract_name))
    #temp  =  []
    #num  =  0
    #while(len(contract_name)>1000):
        #temp.append(contract_name[0:1000])
        #temp[num] = str(tuple(temp[num]))
        #num += 2
        #del contract_name[0:1000]
        #temp = temp+['or A.s_info_windcode in']
        
    #temp.append(str(tuple(contract_name)))
    #contractName = ''.join(temp)
    
    return contractName

# 测试：
if __name__  ==  '__main__':
    db = cx_Oracle.connect('fe','fe','192.168.100.22:1521/winddb')
    cursor = db.cursor()
    startdate = '20100101'
    enddate = '20171231'
    CSV_FilePath = 'E:/work/CTA/fcode_data/'
    vtsymbol_names  = ('IF','IC','IH')
    for vt in vtsymbol_names:
        get_data(cursor, startdate, enddate, vt, CSV_FilePath, save_csv = False, insert2db = True)
    # 插入交易日期
    future_Calendar(startdate, enddate, cursor)
    
    
    # 修改主力合约代码为虚拟代码，原合约代码在domcode里
    #client  =  pymongo.MongoClient('localhost', 27017)
    #for symbol in vtsymbol_names: 
        #db = client['Future_Daily_Db'][symbol]
        #vtSymbol = symbol+'0000.'+db.find_one()['vtSymbol'].split('.')[1]
        #db.update_many({'vtSymbol':{'$nin':[vtSymbol]}},{'$set':{'vtSymbol':vtSymbol}},False)
   

    

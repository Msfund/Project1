# encoding: UTF-8

EXT_Info_File = 'filename'
EXT_Info_Exchange = 'exchange'
EXT_Info_Ticker = 'ticker'
EXT_Info_TickerSim = 'tickerSim'
EXT_Info_TradeDate = 'tradeDate'
EXT_Info_SeriesNum = 'seriesNum'

# period of bar/tick
EXT_PERIOD_1MIN = '1m'
EXT_PERIOD_5MIN = '5m'
EXT_PERIOD_15MIN = '15m'
EXT_PERIOD_60MIN = '60m'
EXT_PERIOD_1DAY = '1d'

# code of exchange
EXT_EXCHANGE_SHFE = 'SHFE' #shang hai Suo
EXT_EXCHANGE_DCE  = 'DCE'  #Da Lian Suo
EXT_EXCHANGE_CZCE = 'CZCE' #ZhengZhou Suo
EXT_EXCHANGE_CFE  = 'CFE'  #Zhong Jin Suo

# code for Commodity/finance Future
EXT_CFE_IF = 'IF'
EXT_CFE_IH = 'IH'
EXT_CFE_IC = 'IC'
EXT_CFE_T  = 'T'
EXT_CFE_TF = 'TF'

#Splitor
EXT_SPLITOR_PORINT = '.'
EXT_SPLITOR_COMMA = ','
EXT_SPLITOR_UNDERSCORE = '_'
EXT_SPLITOR_SEMICOLON = ';'
EXT_SPLITOR_COLON = ':'

#begin end time of futures
EXT_AM_Begin1 = '09:00:00'
EXT_AM_Begin2 = '09:15:00'#T/TF include the aggregate auction trading
EXT_AM_Begin3 = '09:30:00'#IF/IC/IC include the aggregate auction trading
EXT_AM_End    = '11:30:00'
EXT_PM_Begin  = '13:00:00'
EXT_PM_End    = '15:00:00'
EXT_EV_Begin  = '21:00:00'
EXT_EV_End    = '23:30:00'
EXT_TradingTime_Dict = {'AM1':[EXT_AM_Begin1, EXT_AM_End],
                       'AM2':[EXT_AM_Begin2, EXT_AM_End],
                       'AM3':[EXT_AM_Begin3, EXT_AM_End],
                       'PM': [EXT_PM_Begin, EXT_PM_End],
                       'EV': [EXT_EV_Begin, EXT_EV_End]}


#Bar 
EXT_Bar_TickerSim = 'TickerSim'
EXT_Bar_Ticker = 'Ticker'
EXT_Bar_Date = 'Date'
EXT_Bar_Time = 'Time'
EXT_Bar_DateTime = 'DateTime'
EXT_Bar_Open = 'Open'
EXT_Bar_Close = 'Close'
EXT_Bar_High = 'High'
EXT_Bar_Low = 'Low'
EXT_Bar_Volume = 'Volume'
EXT_Bar_OpenInterest = 'OpenInterest'
EXT_Bar_Turnover = 'Turnover'
EXT_Bar_PreSettle = 'PreSettle'
EXT_Bar_Settle = 'Settle'
EXT_Bar_UpLimit = 'UpLimit'
EXT_DownLimit = 'DownLimit'
EXT_AdjFactor = 'AdjFactor'

#citicsf tick file hear, may be change for one TickerSim, so I workaround it by a tickfileheadmap dict.
EXT_Header_CSF1 = 'Time,LastPrice,LVolume,BidPrice,BidVolume,AskPrice,AskVolume,OpenInterest,TradeVolume,LastTurnover,Turnover'
EXT_Header_CSF2 = 'InstrumentID,TradingDay,UpdateTime,LastPrice,BidPrice1,BidVolume1,AskPrice1,AskVolume1,Volume,Turnover,OpenInterest,UpperLimitPrice,LowerLimitPrice,OpenPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest'
                 
#bar rule, how to get the bar data by tick data
EXT_Bar_Rule = { EXT_Bar_Open:'first', EXT_Bar_Close: 'last', EXT_Bar_High:'max',
                      EXT_Bar_Low:'min',EXT_Bar_Volume: 'sum',EXT_Bar_Turnover:'sum', EXT_Bar_OpenInterest:'last'}
#format the header, convert the tick file column to bar header
EXT_Bar_Header = [EXT_Bar_Ticker, EXT_Bar_Open, EXT_Bar_Close, EXT_Bar_High, EXT_Bar_Low, EXT_Bar_Volume, EXT_Bar_Turnover, EXT_Bar_OpenInterest] #EXT_Bar_DateTime is already exsits as dataframe index.
EXT_CFE_Header = {EXT_Bar_Time:'Time', EXT_Bar_Open:'LastPrice', EXT_Bar_Close:'LastPrice', EXT_Bar_High:'LastPrice',
                  EXT_Bar_Low:'LastPrice', EXT_Bar_Volume:'LVolume', EXT_Bar_Turnover:'LastTurnover', EXT_Bar_OpenInterest:'OpenInterest'}
EXT_CFE_Header2 = {EXT_Bar_Time:'UpdateTime', EXT_Bar_Open:'LastPrice', EXT_Bar_Close:'LastPrice', EXT_Bar_High:'LastPrice',
                  EXT_Bar_Low:'LastPrice', EXT_Bar_Volume:'Volume', EXT_Bar_Turnover:'Turnover', EXT_Bar_OpenInterest:'OpenInterest'}
#header mapping to formatted header
EXT_TickFileHeaderMap_Dict = {EXT_Header_CSF1:EXT_CFE_Header, EXT_Header_CSF2:EXT_CFE_Header2}

#deprecated, use the EXT_TickFileHeaderMap_Dict.
EXT_TICK2Bar_Dict = {EXT_EXCHANGE_CFE:EXT_CFE_Header}

## future series Num
EXT_Series_1 = '00'
EXT_Series_2 = '01'
EXT_Series_3 = '02'
EXT_Series_4 = '03'
EXT_Series_5 = '04'
EXT_Series_6 = '05'
EXT_Series_7 = '06'
EXT_Series_8 = '07'
EXT_Series_9 = '08'
EXT_Series_10 = '09'
EXT_Series_11 = '10'
EXT_Series_12 = '11'
EXT_Series_dict = {1:EXT_Series_1, 2:EXT_Series_2, 3:EXT_Series_3, 4:EXT_Series_4,
                   5:EXT_Series_5, 6:EXT_Series_6, 7:EXT_Series_7, 8:EXT_Series_8,
                   9:EXT_Series_9, 10:EXT_Series_10, 11:EXT_Series_11, 12:EXT_Series_12}

EXT_FILE_CSV = '.csv'
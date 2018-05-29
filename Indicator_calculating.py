import talib
from dataUlt import *
import json
# -----------------------TA_Lib documents Page 1------------------------------
# ----------------------------Overlap Studies --------------------------------
with open('Indicator_setting.json','r') as f:
    param = json.load(f)


def BBANDS(data):
    for i in param['BBANDS']['timeperiod']:
        for j in param['BBANDS']['nbdevup']:
            for k in param['BBANDS']['nbdevdn']:
                for l in param['BBANDS']['matype']:
                    data['_'.join(['BBANDS_UPPERBAND',str(i),str(j),str(k),str(l)])], \
                    data['_'.join(['BBANDS_MIDDLEBAND',str(i),str(j),str(k),str(l)])], \
                    data['_'.join(['BBANDS_LOWERBAND',str(i),str(j),str(k),str(l)])] \
                        = talib.BBANDS(data[EXT_Bar_Close].values, timeperiod = i,
                                       nbdevup = j, nbdevdn = k, matype = l)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def DEMA(data):
    for i in param['DEMA']['timeperiod']:
        data['_'.join(['DEMA',str(i)])] = talib.DEMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def EMA(data):
    for i in param['EMA']['timeperiod']:
        data['_'.join(['EMA',str(i)])] = talib.EMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def HT_TRENDLINE(data):
    data['HT_TRENDLINE'] = talib.HT_TRENDLINE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def KAMA(data):
    for i in param['KAMA']['timeperiod']:
        data['_'.join(['KAMA',str(i)])] = talib.KAMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MA(data):
    for i in param['MA']['timeperiod']:
        data['_'.join(['MA',str(i)])] = talib.MA(data[EXT_Bar_Close].values,
                                                 timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MAMA(data):
    ''' fastLimit : Upper limit use in the adaptive algorithm.
        slowLimit : Lower limit use in the adaptive algorithm.
        Valid range from 0.01 to 0.99.'''
    for i in param['MAMA']['fastlimit']:
        for j in param['MAMA']['slowlimit']:
            data['_'.join(['MAMA',str(i),str(j)])], \
            data['_'.join(['FAMA',str(i),str(j)])] \
                = talib.MAMA(data[EXT_Bar_Close].values,
                             fastlimit = i, slowlimit = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# real = MAVP(close, periods, minperiod=2, maxperiod=30, matype=0)

def MIDPOINT(data):
    for i in param['MIDPOINT']['timeperiod']:
        data['_'.join(['MIDPOINT',str(i)])] \
            = talib.MIDPOINT(data[EXT_Bar_Close].values,timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MIDPRICE(data):
    for i in param['MIDPRICE']['timeperiod']:
        data['_'.join(['MIDPRICE',str(i)])] \
            = talib.MIDPRICE(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                             timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SAR(data):
    for i in param['SAR']['acceleration']:
        for j in param['SAR']['maximum']:
            data['_'.join(['SAR',str(i),str(j)])] \
                = talib.SAR(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                            acceleration = i, maximum = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SAREXT(data):
    for i in param['SAREXT']['startvalue']:
        for j in param['SAREXT']['offsetonreverse']:
            for k in param['SAREXT']['accelerationinitlong']:
                for l in param['SAREXT']['accelerationlong']:
                    for m in param['SAREXT']['accelerationmaxlong']:
                        for n in param['SAREXT']['accelerationinitshort']:
                            for o in param['SAREXT']['accelerationshort']:
                                for p in param['SAREXT']['accelerationmaxshort']:
                                    data['_'.join(['SAREXT',str(i),str(j),
                                                   str(k),str(l),str(m),
                                                   str(n),str(o),str(p)])] \
                                        = talib.SAREXT(data[EXT_Bar_High].values,
                                                    data[EXT_Bar_Low].values,
                                                    startvalue = i,
                                                    offsetonreverse = j,
                                                    accelerationinitlong = k,
                                                    accelerationlong = l,
                                                    accelerationmaxlong = m,
                                                    accelerationinitshort = n,
                                                    accelerationshort = o,
                                                    accelerationmaxshort = p)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SMA(data):
    for i in param['SMA']['timeperiod']:
        data['_'.join(['SMA',str(i)])] = talib.SMA(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def T3(data):
    for i in param['T3']['timeperiod']:
        for j in param['T3']['vfactor']:
            data['_'.join(['T3',str(i),str(j)])] \
                = talib.T3(data[EXT_Bar_Close].values,
                            timeperiod = i,
                            vfactor = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TEMA(data):
    for i in param['TEMA']['timeperiod']:
        data['_'.join(['TEMA',str(i)])] = talib.TEMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TRIMA(data):
    for i in param['TRIMA']['timeperiod']:
        data['_'.join(['TRIMA',str(i)])] = talib.TRIMA(data[EXT_Bar_Close].values,
                                                       timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)

    return data

def WMA(data):
    for i in param['WMA']['timeperiod']:
        data['_'.join(['WMA',str(i)])] = talib.WMA(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 2------------------------------
# --------------------------Momentum Indicators-------------------------------

def ADX(data):
    for i in param['ADX']['timeperiod']:
        data['_'.join(['ADX',str(i)])] = talib.ADX(data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ADXR(data):
    for i in param['ADXR']['timeperiod']:
        data['_'.join(['ADXR',str(i)])] = talib.ADXR(data[EXT_Bar_High].values,
                                                     data[EXT_Bar_Low].values,
                                                     data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def APO(data):
    for i in param['APO']['fastperiod']:
        for j in param['APO']['slowperiod']:
            for k in param['APO']['matype']:
                data['_'.join(['APO',str(i),str(j),str(k)])] \
                    = talib.APO(data[EXT_Bar_Close].values,
                                fastperiod = i, slowperiod = j, matype = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def AROON(data):
    for i in param['AROON']['timeperiod']:
        data['_'.join(['ARRONDOWN',str(i)])], \
        data['_'.join(['ARRONUP',str(i)])] \
            = talib.AROON(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                          timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def AROONOSC(data):
    for i in param['AROONOSC']['timeperiod']:
        data['_'.join(['AROONOSC',str(i)])] \
            = talib.AROONOSC(data[EXT_Bar_High].values,
                             data[EXT_Bar_Low].values,
                             timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def BOP(data):
    data['BOP'] = talib.BOP(data[EXT_Bar_Open].values, data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values, data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def CCI(data):
    for i in param['CCI']['timeperiod']:
        data['_'.join(['CCI',str(i)])] \
            = talib.CCI(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                        data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def CMO(data):
    for i in param['CMO']['timeperiod']:
        data['_'.join(['CMO',str(i)])] = talib.CMO(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def DX(data):
    for i in param['DX']['timeperiod']:
        data['_'.join(['DX',str(i)])] \
            = talib.DX(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                        data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MACD(data):
    for i in param['MACD']['fastperiod']:
        for j in param['MACD']['slowperiod']:
            for k in param['MACD']['signalperiod']:
                data['_'.join(['MACD',str(i),str(j),str(k)])], \
                data['_'.join(['MACDSIGNAL',str(i),str(j),str(k)])], \
                data['_'.join(['MACDHIST',str(i),str(j),str(k)])] \
                    = talib.MACD(data[EXT_Bar_Close].values, fastperiod = i,
                                 slowperiod = j, signalperiod = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MACDEXT(data):
    for i in param['MACDEXT']['fastperiod']:
        for j in param['MACDEXT']['fastmatype']:
            for k in param['MACDEXT']['slowperiod']:
                for l in param['MACDEXT']['slowmatype']:
                    for m in param['MACDEXT']['signalperiod']:
                        for n in param['MACDEXT']['signalmatype']:
                            data['_'.join(['MACD',str(i),str(j),str(k),
                                           str(l),str(m),str(n)])], \
                            data['_'.join(['MACDSIGNAL',str(i),str(j),str(k),
                                           str(l),str(m),str(n)])], \
                            data['_'.join(['MACDHIST',str(i),str(j),str(k),
                                           str(l),str(m),str(n)])] \
                                = talib.MACDEXT(data[EXT_Bar_Close].values,
                                                fastperiod = i, fastmatype = j,
                                                slowperiod = k, slowmatype = l,
                                                signalperiod = m, signalmatype = n)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MACDFIX(data):
    for i in param['MACDFIX']['signalperiod']:
        data['_'.join(['MACD',str(i)])], \
        data['_'.join(['MACDSIGNAL',str(i)])], \
        data['_'.join(['MACDHIST',str(i)])] \
            = talib.MACDFIX(data[EXT_Bar_Close].values, signalperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MFI(data):
    for i in param['MFI']['timeperiod']:
        data['_'.join(['MFI',str(i)])] = talib.MFI(data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   data[EXT_Bar_Volume].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MINUS_DI(data):
    for i in param['MINUS_DI']['timeperiod']:
        data['_'.join(['MINUS_DI',str(i)])] = talib.MINUS_DI(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MINUS_DM(data):
    for i in param['MINUS_DM']['timeperiod']:
        data['_'.join(['MINUS_DM',str(i)])] = talib.MINUS_DM(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MOM(data):
    for i in param['MOM']['timeperiod']:
        data['_'.join(['MOM',str(i)])] = talib.MOM(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def PLUS_DI(data):
    for i in param['PLUS_DI']['timeperiod']:
        data['_'.join(['PLUS_DI',str(i)])] = talib.PLUS_DI(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def PLUS_DM(data):
    for i in param['PLUS_DM']['timeperiod']:
        data['_'.join(['PLUS_DM',str(i)])] = talib.PLUS_DM(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def PPO(data):
    for i in param['PPO']['fastperiod']:
        for j in param['PPO']['slowperiod']:
            for k in param['PPO']['matype']:
                data['_'.join(['PPO',str(i),str(j),str(k)])] \
                    = talib.PPO(data[EXT_Bar_Close].values, fastperiod = i,
                                 slowperiod = j, matype = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ROC(data):
    for i in param['ROC']['timeperiod']:
        data['_'.join(['ROC',str(i)])] = talib.ROC(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ROCP(data):
    for i in param['ROCP']['timeperiod']:
        data['_'.join(['ROCP',str(i)])] = talib.ROCP(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ROCR(data):
    for i in param['ROCR']['timeperiod']:
        data['_'.join(['ROCR',str(i)])] = talib.ROCR(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ROCR100(data):
    for i in param['ROCR100']['timeperiod']:
        data['_'.join(['ROCR100',str(i)])] = talib.ROCR100(
                                                data[EXT_Bar_Close].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def RSI(data):
    for i in param['RSI']['timeperiod']:
        data['_'.join(['RSI',str(i)])] = talib.RSI(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def STOCH(data):
    for i in param['STOCH']['fastk_period']:
        for j in param['STOCH']['slowk_period']:
            for k in param['STOCH']['slowk_matype']:
                for l in param['STOCH']['slowd_period']:
                    for m in param['STOCH']['slowd_matype']:
                        data['_'.join(['STOCH_SLOWK',str(i),str(j),str(k),
                                       str(l),str(m)])], \
                        data['_'.join(['STOCH_SLOWD',str(i),str(j),str(k),
                                       str(l),str(m)])] \
                            = talib.STOCH(data[EXT_Bar_High].values,
                                          data[EXT_Bar_Low].values,
                                          data[EXT_Bar_Close].values,
                                          fastk_period = i, slowk_period = j,
                                          slowk_matype = k, slowd_period = l,
                                          slowd_matype = m)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def STOCHF(data):
    for i in param['STOCHF']['fastk_period']:
        for j in param['STOCHF']['fastd_period']:
            for k in param['STOCHF']['fastd_matype']:
                data['_'.join(['STOCHF_FASTK',str(i),str(j),str(k)])], \
                data['_'.join(['STOCHF_FASTD',str(i),str(j),str(k)])] \
                    = talib.STOCHF(data[EXT_Bar_High].values,
                                   data[EXT_Bar_Low].values,
                                   data[EXT_Bar_Close].values,
                                   fastk_period = i, fastd_period = j,
                                   fastd_matype = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def STOCHRSI(data):
    for i in param['STOCHRSI']['timeperiod']:
        for j in param['STOCHRSI']['fastk_period']:
            for k in param['STOCHRSI']['fastd_period']:
                for l in param['STOCHRSI']['fastd_matype']:
                    data['_'.join(['STOCHRSI_FASTK',str(i),str(j),str(k),str(l)])], \
                    data['_'.join(['STOCHRSI_FASTD',str(i),str(j),str(k),str(l)])] \
                        = talib.STOCHRSI(data[EXT_Bar_Close].values,
                                         timeperiod = i, fastk_period = j,
                                         fastd_period = k, fastd_matype = l)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TRIX(data):
    for i in param['TRIX']['timeperiod']:
        data['_'.join(['TRIX',str(i)])] = talib.TRIX(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ULTOSC(data):
    for i in param['ULTOSC']['timeperiod1']:
        for j in param['ULTOSC']['timeperiod2']:
            for k in param['ULTOSC']['timeperiod3']:
                data['_'.join(['ULTOSC',str(i),str(j),str(k)])] \
                    = talib.ULTOSC(data[EXT_Bar_High].values,
                                   data[EXT_Bar_Low].values,
                                   data[EXT_Bar_Close].values,
                                   timeperiod1 = i,
                                   timeperiod2 = j,
                                   timeperiod3 = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def WILLR(data):
    for i in param['WILLR']['timeperiod']:
        data['_'.join(['WILLR',str(i)])]\
            = talib.WILLR(data[EXT_Bar_High].values,
                          data[EXT_Bar_Low].values,
                          data[EXT_Bar_Close].values,
                          timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 3------------------------------
# --------------------------Volume Indicators---------------------------------

def AD(data):
    data['AD'] = talib.AD(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                          data[EXT_Bar_Close].values, data[EXT_Bar_Volume].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ADOSC(data):
    for i in param['ADOSC']['fastperiod']:
        for j in param['ADOSC']['slowperiod']:
            data['_'.join(['ADOSC',str(i),str(j)])] \
                = talib.ADOSC(data[EXT_Bar_High].values,
                              data[EXT_Bar_Low].values,
                              data[EXT_Bar_Close].values,
                              data[EXT_Bar_Volume].values,
                              fastperiod = i,
                              slowperiod = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def OBV(data):
    data['OBV'] = talib.OBV(data[EXT_Bar_Close].values,
                            data[EXT_Bar_Volume].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 4------------------------------
# ----------------------------Cycle Indicator---------------------------------

def HT_DCPERIOD(data):
    data['HT_DCPERIOD'] = talib.HT_DCPERIOD(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def HT_DCPHASE(data):
    data['HT_DCPHASE'] = talib.HT_DCPHASE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)

    return data

def HT_PHASOR(data):
    data['inphase'], data['quadrature'] = talib.HT_PHASOR(
                                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def HT_SINE(data):
    data['sine'], data['leadsine'] = talib.HT_SINE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def HT_TRENDMODE(data):
    data['HT_TRENDMODE'] = talib.HT_TRENDMODE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 5------------------------------
# ----------------------------Price Transform---------------------------------

def AVGPRICE(data):
    data['AVGPRICE'] = talib.AVGPRICE(
                            data[EXT_Bar_Open].values,
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values,
                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MEDPRICE(data):
    data['MEDPRICE'] = talib.MEDPRICE(
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TYPPRICE(data):
    data['TYPPRICE'] = talib.TYPPRICE(
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values,
                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def WCLPRICE(data):
    data['WCLPRICE'] = talib.WCLPRICE(
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values,
                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 6------------------------------
# ------------------------Volatility Indicators-------------------------------
def ATR(data):
    for i in param['ATR']['timeperiod']:
        data['_'.join(['ATR',str(i)])] = talib.ATR(data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def NATR(data):
    for i in param['NATR']['timeperiod']:
        data['_'.join(['NATR',str(i)])] = talib.NATR(data[EXT_Bar_High].values,
                                                     data[EXT_Bar_Low].values,
                                                     data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TRANGE(data):
    data['TRANGE'] = talib.TRANGE(data[EXT_Bar_High].values,
                                  data[EXT_Bar_Low].values,
                                  data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 7------------------------------
# ------------------------Statistic Functions-------------------------------

def BETA(data):
    for i in param['BETA']['timeperiod']:
        data['_'.join(['BETA',str(i)])] = talib.BETA(data[EXT_Bar_High].values,
                                                     data[EXT_Bar_Low].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def CORREL(data):
    for i in param['CORREL']['timeperiod']:
        data['_'.join(['CORREL',str(i)])] = talib.CORREL(
                                                data[EXT_Bar_High].values,
                                                data[EXT_Bar_Low].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def LINEARREG(data):
    for i in param['LINEARREG']['timeperiod']:
        data['_'.join(['LINEARREG',str(i)])] = talib.LINEARREG(
                                                    data[EXT_Bar_Close].values,
                                                    timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def LINEARREG_ANGLE(data):
    for i in param['LINEARREG_ANGLE']['timeperiod']:
        data['_'.join(['LINEARREG_ANGLE',str(i)])] \
            = talib.LINEARREG_ANGLE(data[EXT_Bar_Close].values,timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def LINEARREG_INTERCEPT(data):
    for i in param['LINEARREG_INTERCEPT']['timeperiod']:
        data['_'.join(['LINEARREG_INTERCEPT',str(i)])] \
            = talib.LINEARREG_INTERCEPT(data[EXT_Bar_Close].values,
                                        timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def LINEARREG_SLOPE(data):
    for i in param['LINEARREG_SLOPE']['timeperiod']:
        data['_'.join(['LINEARREG_SLOPE',str(i)])] \
            = talib.LINEARREG_SLOPE(data[EXT_Bar_Close].values,timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def STDDEV(data):
    for i in param['STDDEV']['timeperiod']:
        for j in param['STDDEV']['nbdev']:
            data['_'.join(['STDDEV',str(i),str(j)])] \
                = talib.STDDEV(data[EXT_Bar_Close].values,
                               timeperiod = i, nbdev = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TSF(data):
    for i in param['TSF']['timeperiod']:
        data['_'.join(['TSF',str(i)])] = talib.TSF(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def VAR(data):
    for i in param['VAR']['timeperiod']:
        for j in param['VAR']['nbdev']:
            data['_'.join(['stddev',str(i),str(j)])] \
                = talib.VAR(data[EXT_Bar_Close].values,
                            timeperiod = i, nbdev = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 7------------------------------
# ----------------------------Math Transform----------------------------------

def ACOS(data):
    data['ACOS'] = talib.ACOS(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ASIN(data):
    data['ASIN'] = talib.ASIN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ATAN(data):
    data['ATAN'] = talib.ATAN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def CEIL(data):
    data['CEIL'] = talib.CEIL(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def COS(data):
    data['COS'] = talib.COS(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def COSH(data):
    data['COSH'] = talib.COSH(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def EXP(data):
    data['EXP'] = talib.EXP(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def FLOOR(data):
    data['FLOOR'] = talib.FLOOR(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def LN(data):
    data['LN'] = talib.LN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def LOG10(data):
    data['LOG10'] = talib.LOG10(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SIN(data):
    data['SIN'] = talib.SIN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SINH(data):
    data['SINH'] = talib.SINH(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SQRT(data):
    data['SQRT'] = talib.SQRT(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TAN(data):
    data['TAN'] = talib.TAN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def TANH(data):
    data['TANH'] = talib.TANH(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 8------------------------------
# ----------------------------Math Operators----------------------------------

def ADD(data):
    data['ADD'] = talib.ADD(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def DIV(data):
    data['DIV'] = talib.DIV(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MAX(data):
    for i in param['MAX']['timeperiod']:
        data['_'.join(['MAX',str(i)])] = talib.MAX(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MAXINDEX(data):
    for i in param['MAXINDEX']['timeperiod']:
        data['_'.join(['MAXINDEX',str(i)])] = talib.MAXINDEX(
                                                data[EXT_Bar_Close].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MIN(data):
    for i in param['MIN']['timeperiod']:
        data['_'.join(['MIN',str(i)])] = talib.MIN(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MININDEX(data):
    for i in param['MININDEX']['timeperiod']:
        data['_'.join(['MININDEX',str(i)])] = talib.MININDEX(
                                                data[EXT_Bar_Close].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MINMAX(data):
    for i in param['MINMAX']['timeperiod']:
        data['_'.join(['min',str(i)])], data['_'.join(['max',str(i)])]  \
            = talib.MINMAX(data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MINMAXINDEX(data):
    for i in param['MINMAXINDEX']['timeperiod']:
        data['_'.join(['minidx',str(i)])], data['_'.join(['maxidx',str(i)])]  \
            = talib.MINMAXINDEX(data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MULT(data):
    data['MULT'] = talib.MULT(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SUB(data):
    data['SUB'] = talib.SUB(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def SUM(data):
    for i in param['SUM']['timeperiod']:
        data['_'.join(['SUM',str(i)])] = talib.SUM(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

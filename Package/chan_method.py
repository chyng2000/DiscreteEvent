import pandas as pd
from os import system, name

def load_data(encoder = "cp1252", rawdata = "Input"):
    # Import data
    df1 = pd.read_csv("./" + rawdata + "/RawDaily.csv",usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],encoding=encoder).dropna(how = 'any')
    df1w = pd.read_csv("./" + rawdata + "/RawWeekly.csv",usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],encoding=encoder).dropna(how = 'any')
    df1m = pd.read_csv("./" + rawdata + "/RawMonthly.csv",usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],encoding=encoder).dropna(how = 'any')

    df2 = pd.read_csv("./" + rawdata + "/Date.csv",usecols=[0,1,2,3],encoding=encoder).drop_duplicates(subset=['dd'],keep='last').dropna(how = 'any')
    df3 = pd.read_csv("./" + rawdata + "/Flow.csv",usecols=[0,1,2,3,4,5,6,7,8,9,10],encoding=encoder).drop_duplicates(subset=['product','step','eqp'],keep='last').fillna({'hc_group':"no group"}).dropna(how = 'any')
    df4 = pd.read_csv("./" + rawdata + "/Off.csv",usecols=[0,1],encoding=encoder).drop_duplicates(subset=['dd','eqp'],keep='last').dropna(how = 'any')
    df5 = pd.read_csv("./" + rawdata + "/Eqp.csv",usecols=[0,1],encoding=encoder).drop_duplicates(subset=['eqp'],keep='last').dropna(how = 'any')
    df6 = pd.read_csv("./" + rawdata + "/Update.csv",usecols=[0,1,2,3,4,5,6],encoding=encoder).dropna(subset=['item','startdate','newvalue','status'])
    df7 = pd.read_csv("./" + rawdata + "/Map.csv",usecols=[1,2],encoding=encoder).drop_duplicates(subset=['partnumber'],keep='last').dropna(how = 'any')

    # Melt forecast table
    df_DailyForecast = df1.melt(['check','product','partnumber','stepStart','stepEnd'], var_name='dd', value_name='qty').dropna(how = 'any')
    df_DailyForecast = df_DailyForecast.groupby(['product','dd','stepStart','stepEnd'], as_index=False)['qty'].sum()
    df_WeeklyForecast = df1w.melt(['check','product','partnumber','stepStart','stepEnd'], var_name='cw', value_name='qty').dropna(how = 'any')
    df_WeeklyForecast = df_WeeklyForecast.groupby(['product','cw','stepStart','stepEnd'], as_index=False)['qty'].sum()
    df_MonthlyForecast = df1m.melt(['check','product','partnumber','stepStart','stepEnd'], var_name='cm', value_name='qty').dropna(how = 'any')
    df_MonthlyForecast = df_MonthlyForecast.groupby(['product','cm','stepStart','stepEnd'], as_index=False)['qty'].sum()
    dict_df = {'RawDataForDailyCT':df1, 'RawDataForWeekly':df1w, 'RawDataForMonthly':df1m, 'Date':df2, 'Flow':df3, 'Off':df4, 'Eqp':df5,
               'Update':df6, 'DataForDaily':df_DailyForecast, 'DataForWeekly':df_WeeklyForecast, 'DataForMonthly':df_MonthlyForecast, 'Mapping':df7}
    return dict_df


def clear():
    # for windows
    if name == 'nt':
        _ = system('cls')

        # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')
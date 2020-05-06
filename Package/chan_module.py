import pandas as pd
import numpy as np

class EqpCalendarCreator(object):
    def __init__(self, TableOfEqp, TableOfOff, TableOfCalendar):
        self.__df_Eqp = TableOfEqp[['eqp']]
        self.__df_DayAvailable = TableOfCalendar[['cm', 'cw', 'dd']]
        self.__df_EqpOff = TableOfOff

        self.__df_EqpOff['off'] = 1
        self.__df_Eqp['tmp'] = 1
        self.__df_DayAvailable['on'] = 1
        self.__df_DayAvailable['tmp'] = 1
        self.__df_DayAvailableOfEqp = pd.merge(self.__df_Eqp, self.__df_DayAvailable, on=['tmp']).drop('tmp', axis=1)

    def getEqpDailyCalendar(self):
        df_EqpCalenderByDay = pd.merge(self.__df_DayAvailableOfEqp, self.__df_EqpOff, on=['dd', 'eqp'], how='left')
        df_EqpCalenderByDay['off'] = df_EqpCalenderByDay[['off']].fillna(value=0)
        df_EqpCalenderByDay['day_day'] = df_EqpCalenderByDay['on'] - df_EqpCalenderByDay['off']
        df_EqpCalenderByDay = df_EqpCalenderByDay[['eqp', 'cm', 'cw', 'dd', 'day_day']]
        return df_EqpCalenderByDay

    def getEqpWeeklyCalendar(self):
        df_EqpCalenderByWeek = self.getEqpDailyCalendar().groupby(['cw', 'eqp'], as_index=False)['day_day'].sum()
        df_EqpCalenderByWeek = df_EqpCalenderByWeek.rename(columns={"day_day": "day_week"})
        return df_EqpCalenderByWeek

    def getEqpMonthlyCalendar(self):
        df_EqpCalenderByMonth = self.getEqpDailyCalendar().groupby(['cm', 'eqp'], as_index=False)['day_day'].sum()
        df_EqpCalenderByMonth = df_EqpCalenderByMonth.rename(columns={"day_day": "day_month"})
        return df_EqpCalenderByMonth


class Model(object):
    def __init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                 EqpWeeklyOrMonthlyCalendar):
        self.df_Forecast = TableOfForecast  # need change
        self.df_Flow = TableOfFlow[['product', 'step', 'process', 'eqp', 'oee', 'uph','pass','hc_group', 'mmr']]
        self.df_Eqp = TableOfEqp
        self.df_Update = TableOfUpdate
        self.df_EqpCalenderByDay = EqpDailyCalendar
        self.df_EqpCalenderByWeekOrMonth = EqpWeeklyOrMonthlyCalendar  # need change

    def getEqDaily(self):
        df_dailyEQ = pd.merge(self.df_Forecast, self.df_Flow, on=['product'], how='left')
        df_dailyEQ = df_dailyEQ[(df_dailyEQ.step >= df_dailyEQ.stepStart) & (df_dailyEQ.step <= df_dailyEQ.stepEnd)]
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByDay, on=['eqp', 'cw'], how='left')  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByWeekOrMonth, on=['eqp', 'cw'],
                              how='left')  # need change
        df_dailyEQ['qty_day'] = (df_dailyEQ['qty'] / df_dailyEQ['day_week']) * df_dailyEQ['day_day']  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_Eqp, on=['eqp'], how='left')
        df_dailyEQ = df_dailyEQ[
            ['product', 'cm', 'cw', 'dd', 'step', 'process', 'eqp', 'oee', 'uph','pass', 'hc_group', 'mmr', 'day_day',
             'availeqp', 'qty_day']]
        return df_dailyEQ

    def getEqDailyUpdated(self):
        df8 = self.df_Update
        df_dailyEQ = self.getEqDaily()

        for i in df8.index:

            if df8.at[i, 'item'] == 'uph' and df8.at[i, 'product'] == 'All' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[(df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (df_dailyEQ['step'] == df8.at[i, 'step']) & (
                            df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'uph'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'oee' and df8.at[i, 'product'] == 'All' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[(df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (df_dailyEQ['step'] == df8.at[i, 'step']) & (
                            df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'oee'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'pass' and df8.at[i, 'product'] == 'All' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[(df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (df_dailyEQ['step'] == df8.at[i, 'step']) & (
                        df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'pass'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'mmr' and df8.at[i, 'product'] == 'All' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[(df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (df_dailyEQ['step'] == df8.at[i, 'step']) & (
                            df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'mmr'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'uph' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[
                    (df_dailyEQ['product'] == df8.at[i, 'product']) & (df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (
                                df_dailyEQ['step'] == df8.at[i, 'step']) & (
                                df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'uph'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'oee' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[
                    (df_dailyEQ['product'] == df8.at[i, 'product']) & (df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (
                                df_dailyEQ['step'] == df8.at[i, 'step']) & (
                                df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'oee'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'pass' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[
                    (df_dailyEQ['product'] == df8.at[i, 'product']) & (df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (
                                df_dailyEQ['step'] == df8.at[i, 'step']) & (
                                df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'pass'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'mmr' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[
                    (df_dailyEQ['product'] == df8.at[i, 'product']) & (df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (
                                df_dailyEQ['step'] == df8.at[i, 'step']) & (
                                df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'mmr'] = df8.at[i, 'newvalue']

            elif df8.at[i, 'item'] == 'availeqp' and df8.at[i, 'status'] == 'Plan':
                df_dailyEQ.loc[(df_dailyEQ['eqp'] == df8.at[i, 'eqp']) & (
                            df_dailyEQ['dd'] >= df8.at[i, 'startdate']), 'availeqp'] = df8.at[i, 'newvalue']

        df_dailyEQ['hour'] = 24
        df_dailyEQ['qty_day'] = df_dailyEQ['qty_day']*df_dailyEQ['pass']
        df_dailyEQ.loc[df_dailyEQ.day_day == 0, ['availeqp', 'oee', 'uph','pass', 'mmr', 'hour']] = np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
        return df_dailyEQ

    def getDailyRequirement(self):
        df_dailyEQ = pd.pivot_table(self.getEqDailyUpdated(),index=['product','cm','cw','dd','step','process','eqp','hc_group'],
                                    aggfunc={'oee':np.mean,"uph":np.mean,"pass":np.mean,"mmr":np.mean, "availeqp":np.mean,
                                             "qty_day":np.sum,"hour":np.sum}).reset_index()
        df_dailyEQ['req'] = df_dailyEQ['qty_day']/(df_dailyEQ['uph']*df_dailyEQ['oee']*df_dailyEQ['hour'])
        df_dailyEQ['util'] = df_dailyEQ['req']/df_dailyEQ['availeqp']
        df_dailyEQ['fte_required'] = df_dailyEQ['req'] / df_dailyEQ['mmr']
        df_dailyAggregate = pd.pivot_table(df_dailyEQ,index=['eqp','dd'],aggfunc={'qty_day':np.sum,"util":np.sum})
        df_dailyAggregate['capacity']=df_dailyAggregate['qty_day']/df_dailyAggregate['util']
        df_dailyAggregate = df_dailyAggregate.rename(columns = {'qty_day':'total_qty', 'util':'total_util'})
        df_dailyEQ = pd.merge(df_dailyEQ, df_dailyAggregate, on=['eqp','dd'],how='left')
        df_dailyEQ = df_dailyEQ.replace([np.inf, -np.inf], np.nan)

        return df_dailyEQ

    def getWeeklyRequirement(self):
        df_weeklyEQ = pd.pivot_table(self.getEqDailyUpdated(),index=['product','cw','step','process','eqp','hc_group'],
                                    aggfunc={'oee':np.mean,"uph":np.mean,"pass":np.mean,"mmr":np.mean, "availeqp":np.mean,
                                             "qty_day":np.sum,"hour":np.sum}).reset_index()
        df_weeklyEQ['req'] = df_weeklyEQ['qty_day']/(df_weeklyEQ['uph']*df_weeklyEQ['oee']*df_weeklyEQ['hour'])
        df_weeklyEQ['util'] = df_weeklyEQ['req']/df_weeklyEQ['availeqp']
        df_weeklyEQ['fte_required'] = df_weeklyEQ['req'] / df_weeklyEQ['mmr']
        df_weeklyAggregate = pd.pivot_table(df_weeklyEQ,index=['eqp','cw'],aggfunc={'qty_day':np.sum,"util":np.sum})
        df_weeklyAggregate['capacity']=df_weeklyAggregate['qty_day']/df_weeklyAggregate['util']
        df_weeklyAggregate = df_weeklyAggregate.rename(columns = {'qty_day':'total_qty', 'util':'total_util'})
        df_weeklyEQ = pd.merge(df_weeklyEQ, df_weeklyAggregate, on=['eqp','cw'],how='left')
        df_weeklyEQ = df_weeklyEQ.replace([np.inf, -np.inf], np.nan)

        return df_weeklyEQ

    def getMonthlyRequirement(self):
        df_monthlyEQ = pd.pivot_table(self.getEqDailyUpdated(),index=['product','cm','step','process','eqp','hc_group'],
                                    aggfunc={'oee':np.mean,"uph":np.mean,"pass":np.mean,"mmr":np.mean, "availeqp":np.mean,
                                             "qty_day":np.sum,"hour":np.sum}).reset_index()
        df_monthlyEQ['req'] = df_monthlyEQ['qty_day']/(df_monthlyEQ['uph']*df_monthlyEQ['oee']*df_monthlyEQ['hour'])
        df_monthlyEQ['util'] = df_monthlyEQ['req']/df_monthlyEQ['availeqp']
        df_monthlyEQ['fte_required'] = df_monthlyEQ['req'] / df_monthlyEQ['mmr']
        df_monthlyAggregate = pd.pivot_table(df_monthlyEQ,index=['eqp','cm'],aggfunc={'qty_day':np.sum,"util":np.sum})
        df_monthlyAggregate['capacity']=df_monthlyAggregate['qty_day']/df_monthlyAggregate['util']
        df_monthlyAggregate = df_monthlyAggregate.rename(columns = {'qty_day':'total_qty', 'util':'total_util'})
        df_monthlyEQ = pd.merge(df_monthlyEQ, df_monthlyAggregate, on=['eqp','cm'],how='left')
        df_monthlyEQ = df_monthlyEQ.replace([np.inf, -np.inf], np.nan)
        return df_monthlyEQ


class ModelUsingDayCT(Model):
    def __init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                 EqpWeeklyOrMonthlyCalendar):
        Model.__init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                       EqpWeeklyOrMonthlyCalendar)

    # Override
    def getEqDaily(self):
        df_ctForcast = pd.pivot_table(self.df_Forecast, index=['product', 'dd', 'step', 'eqp'],
                                      aggfunc={"qty_unit": np.sum}).reset_index()
        df_ctForcast = df_ctForcast.rename(columns={"qty_unit": "qty_day"})
        min_dd = np.sort(df_ctForcast.dd.unique())[0]
        max_dd = np.sort(df_ctForcast.dd.unique())[-1]
        df_EqpCalenderByDay = self.df_EqpCalenderByDay
        df_EqpCalenderByDay = df_EqpCalenderByDay[
            (df_EqpCalenderByDay.dd >= min_dd) & (df_EqpCalenderByDay.dd <= max_dd)]

        df_dailyEQ = df_ctForcast[['product']].drop_duplicates(subset=['product'], keep='last')
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_Flow, on=['product'], how='left')
        df_dailyEQ = pd.merge(df_dailyEQ, df_EqpCalenderByDay, on=['eqp'], how='left')
        df_dailyEQ = pd.merge(df_dailyEQ, df_ctForcast, on=['product', 'dd', 'step', 'eqp'], how='left')
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_Eqp, on=['eqp'], how='left')
        df_dailyEQ = df_dailyEQ[
            ['product', 'cm', 'cw', 'dd', 'step', 'process', 'eqp', 'oee', 'uph','pass', 'hc_group', 'mmr', 'day_day',
             'availeqp', 'qty_day']]
        df_dailyEQ = df_dailyEQ.fillna({'qty_day': 0})
        return df_dailyEQ


class ModelUsingDay(Model):

    def __init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                 EqpWeeklyOrMonthlyCalendar):
        Model.__init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                       EqpWeeklyOrMonthlyCalendar)

    # Override
    def getEqDaily(self):
        df_dailyEQ = pd.merge(self.df_Forecast, self.df_Flow, on=['product'], how='left')
        df_dailyEQ = df_dailyEQ[(df_dailyEQ.step >= df_dailyEQ.stepStart) & (df_dailyEQ.step <= df_dailyEQ.stepEnd)]
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByDay, on=['eqp', 'dd'], how='left')  # need change
        #         df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByWeekOrMonth, on=['eqp','dd'],how='left') # need change
        df_dailyEQ['qty_day'] = (df_dailyEQ['qty'] / df_dailyEQ['day_day']) * df_dailyEQ['day_day']  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_Eqp, on=['eqp'], how='left')
        df_dailyEQ = df_dailyEQ[
            ['product', 'cm', 'cw', 'dd', 'step', 'process', 'eqp', 'oee', 'uph','pass', 'hc_group', 'mmr', 'day_day',
             'availeqp', 'qty_day']]
        return df_dailyEQ


class ModelUsingWeek(Model):

    def __init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                 EqpWeeklyOrMonthlyCalendar):
        Model.__init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                       EqpWeeklyOrMonthlyCalendar)

    # Override
    def getEqDaily(self):
        df_dailyEQ = pd.merge(self.df_Forecast, self.df_Flow, on=['product'], how='left')
        df_dailyEQ = df_dailyEQ[(df_dailyEQ.step >= df_dailyEQ.stepStart) & (df_dailyEQ.step <= df_dailyEQ.stepEnd)]
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByDay, on=['eqp', 'cw'], how='left')  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByWeekOrMonth, on=['eqp', 'cw'],
                              how='left')  # need change
        df_dailyEQ['qty_day'] = (df_dailyEQ['qty'] / df_dailyEQ['day_week']) * df_dailyEQ['day_day']  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_Eqp, on=['eqp'], how='left')
        df_dailyEQ = df_dailyEQ[
            ['product', 'cm', 'cw', 'dd', 'step', 'process', 'eqp', 'oee', 'uph','pass', 'hc_group', 'mmr', 'day_day',
             'availeqp', 'qty_day']]
        return df_dailyEQ


class ModelUsingMonth(Model):

    def __init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                 EqpWeeklyOrMonthlyCalendar):
        Model.__init__(self, TableOfForecast, TableOfFlow, TableOfEqp, TableOfUpdate, EqpDailyCalendar,
                       EqpWeeklyOrMonthlyCalendar)

    # Override
    def getEqDaily(self):
        df_dailyEQ = pd.merge(self.df_Forecast, self.df_Flow, on=['product'], how='left')
        df_dailyEQ = df_dailyEQ[(df_dailyEQ.step >= df_dailyEQ.stepStart) & (df_dailyEQ.step <= df_dailyEQ.stepEnd)]
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByDay, on=['eqp', 'cm'], how='left')  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_EqpCalenderByWeekOrMonth, on=['eqp', 'cm'],
                              how='left')  # need change
        df_dailyEQ['qty_day'] = (df_dailyEQ['qty'] / df_dailyEQ['day_month']) * df_dailyEQ['day_day']  # need change
        df_dailyEQ = pd.merge(df_dailyEQ, self.df_Eqp, on=['eqp'], how='left')
        df_dailyEQ = df_dailyEQ[
            ['product', 'cm', 'cw', 'dd', 'step', 'process', 'eqp', 'oee', 'uph','pass', 'hc_group', 'mmr', 'day_day',
             'availeqp', 'qty_day']]
        return df_dailyEQ


class manual_work_model():
    def __init__(self, forecast, calendar, shutdown, uom, process):
        self.df1 = forecast
        self.df2 = calendar
        self.df3 = shutdown
        self.df4 = uom
        self.df5 = process

    def get_report(self):
        temp01 = self.df4[(self.df4["ProdName"] == 'Nt Product Related')].drop(
            columns=['ProcessGroup', 'UOM', 'qty/UOM', 'UOM/Day']).drop_duplicates(
            subset=['Area', 'ProdName']).reset_index(drop=True)
        temp02 = self.df1.groupby(['Plan', 'Area', 'CM'], as_index=False).sum()
        temp02 = temp02[(temp02["MonthQty"] > 0)].drop(columns=['MonthQty'])
        temp03 = pd.merge(temp02, temp01, on=['Area'], how='left')
        temp03['PartNumber'] = temp03['ProdName']
        temp03['MonthQty'] = 1
        header_list = ['Plan', 'Area', 'ProdName', 'PartNumber', 'CM', 'MonthQty']
        temp03 = temp03.reindex(columns=header_list)
        t0 = self.df1.append(temp03, ignore_index=True)

        t1 = pd.merge((pd.merge(t0, self.df2, on=['CM'], how='left')), self.df3, on=['CM', 'Area'], how='left')
        t1['ShutDown'].fillna(value=0, inplace=True)
        t1['Working Day/Month'] = t1['Day Available/Month'] - t1['ShutDown']
        t2 = pd.merge(t1, self.df4, on=['Area', 'ProdName'], how='left')
        t3 = pd.merge(t2, self.df5, on=['Area', 'ProcessGroup', 'UOM'], how='left')
        t3['Frequency'] = t3['MonthQty'] / t3['qty/UOM']
        t3.loc[(t3['UOM/Day'].isnull() == False), 'Frequency'] = t3['Working Day/Month'] * t3['UOM/Day']
        t3['TotalTime(s)'] = t3['Frequency'] * t3['ElementTime(s)'] / t3['Allowance']
        t3['TotalTime(hr)'] = t3['TotalTime(s)'] / 3600
        t3_less = t3[(t3["Frequency"] != 0)]
        t3_less = t3_less.replace([np.inf, -np.inf], np.nan)

        t4 = t0.groupby(['Plan', 'Area', 'ProdName', 'CM'], as_index=False)['MonthQty'].sum()
        t5 = pd.merge((pd.merge(t4, self.df2, on=['CM'], how='left')), self.df3, on=['CM', 'Area'], how='left')
        t5['ShutDown'].fillna(value=0, inplace=True)
        t5['Working Day/Month'] = t5['Day Available/Month'] - t5['ShutDown']
        t6 = pd.merge(t5, self.df4, on=['Area', 'ProdName'], how='left')
        t7 = pd.merge(t6, self.df5, on=['Area', 'ProcessGroup', 'UOM'], how='left')
        t7['Frequency'] = t7['MonthQty'] / t7['qty/UOM']
        t7.loc[(t7['UOM/Day'].isnull() == False), 'Frequency'] = t7['Working Day/Month'] * t7['UOM/Day']
        t7['TotalTime(s)'] = t7['Frequency'] * t7['ElementTime(s)'] / t7['Allowance']
        t7['TotalTime(hr)'] = t7['TotalTime(s)'] / 3600
        c1 = t3.groupby(['Area', 'PartNumber']).nunique()
        header_newlist = ['Plan', 'ProdName', 'CM', 'ProcessGroup', 'UOM', 'qty/UOM', 'UOM/Day', 'Process', 'Step']
        c1 = c1.reindex(columns=header_newlist)
        c1 = c1.reset_index()
        c2 = self.df5.groupby(['Area']).nunique()
        header_newlist2 = ['Step']
        c2 = c2.reindex(columns=header_newlist2)
        c2 = c2.reset_index()

        c3 = t3.groupby(['Plan', 'Area', 'CM']).nunique()
        header_newlist3 = ['Step']
        c3 = c3.reindex(columns=header_newlist3)
        c3 = c3.reset_index()

        c4 = pd.merge(c2, c3, on=['Area'], how='left')
        header_newlist4 = ['Plan', 'Area', 'CM', 'Step_x', 'Step_y']
        c4 = c4.reindex(columns=header_newlist4)
        c4['CheckStep'] = c4['Step_x'] - c4['Step_y']

        RawQty = self.df1.groupby(['Plan', 'Area', 'CM', 'ProdName'], as_index=False).sum()
        c5 = t7.groupby(['Plan', 'Area', 'ProdName', 'CM'], as_index=False)['MonthQty'].mean()
        c5 = pd.merge(RawQty, c5, on=['Plan', 'Area', 'ProdName', 'CM'], how='left')
        c5['CheckQty'] = c5['MonthQty_x'] - c5['MonthQty_y']

        report_dict = {}
        report_dict['manhour'] = t3_less
        report_dict['qtychecked'] = c5
        report_dict['stepchecked'] = c4
        return report_dict


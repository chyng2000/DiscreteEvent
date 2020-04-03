import pandas as pd

import numpy as np
# custom rounder


def round_down(num, divisor):
    return num - (num%divisor)


class IteratorInfoCreator(object):
    def __init__(self, TableOfRawDaily, TableOfDate, TableOfFlow, TableOfOff):
        self.df_forecast = TableOfRawDaily
        self.df_date = TableOfDate
        self.df_flow = TableOfFlow.sort_values(by=['step'])
        self.df_off = TableOfOff

    def createEntityDictionary(self):
        df1_1 = self.df_forecast.melt(['check', 'stepStart', 'stepEnd', 'product', 'partnumber'], var_name='dd',
                                      value_name='qty').dropna(how='any')
        df1_1 = df1_1[(df1_1['qty'] > 0)]
        df1_1 = pd.merge(df1_1, self.df_date, on=['dd'], how='left')
        df1_1 = df1_1.groupby(['product', 'datestamp', 'stepStart', 'stepEnd'], as_index=False)['qty'].sum()
        df1_1['key'] = df1_1['product'] + "_" + df1_1['datestamp'].astype(str) + "_" + df1_1['stepStart'].astype(str)
        dict_entity = df1_1.to_dict(orient='records')
        return dict_entity

    def createFlowDictionary(self):
        dict_flow = {}
        for row in self.df_flow.itertuples():
            if row.product not in dict_flow.keys():
                dict_flow[row.product] = {row.step: [{'eqp': row.eqp, 'ct': row.ct, 'die': row.die}]}
            elif row.product in dict_flow.keys() and row.step not in dict_flow[row.product].keys():
                dict_flow[row.product][row.step] = [{'eqp': row.eqp, 'ct': row.ct, 'die': row.die}]
            else:
                dict_flow[row.product][row.step].append({'eqp': row.eqp, 'ct': row.ct, 'die': row.die})
        return dict_flow

    def createOffDictionary(self):
        dict_off = {}
        df4_1 = self.df_off.dropna(how='any')
        df4_1 = pd.merge(df4_1, self.df_date, on=['dd'], how='left')
        for row in df4_1.itertuples():
            if row.datestamp not in dict_off.keys():
                dict_off[row.datestamp] = [row.eqp]
            else:
                dict_off[row.datestamp].append(row.eqp)

        return dict_off


class Iterator:

    def __init__(self, product, date, step_start, step_end, qty, flow, off, key):
        self.__product = product
        self.__date = date
        self.__startStep = step_start
        self.__endStep = step_end
        self.__qty = qty
        self.__flow = flow
        self.__off = off
        self.__key = key
        self.__startDate = date
        self.__log = {"product": [], "cycletime": [], "date": [], "step": [], "eqp": [], "qty": [], "die": [],
                      "key": []}
        self.__list = []

    def iterate(self, debug=False):
        maxDate = 0
        startDate = self.__date


        for k in self.__flow.keys():
            startDate = max(maxDate, startDate)
            for i in self.__flow[k]:
                balanceTime = i["ct"]
                cycleTime = i["ct"]
                eqpDate = startDate
                while balanceTime > 0:
                    if debug == True:
                        self.__list.append("current step:{}, eqp:{}, balanceTime:{}, eqp start date:{}, enviroment date:{}, id:{}".
                                           format(k, i["eqp"], balanceTime, startDate, eqpDate,self.__key))
                    if (round_down(eqpDate, 1440) in self.__off.keys()) and (i["eqp"] in self.__off[round_down(eqpDate, 1440)]):
                        pass
                    else:
                        if balanceTime == cycleTime:
                            self.__log["product"].append(self.__product)
                            self.__log["cycletime"].append(balanceTime)
                            self.__log["date"].append(eqpDate)
                            self.__log["step"].append(k)
                            self.__log["eqp"].append(i['eqp'])
                            self.__log["qty"].append(self.__qty)
                            self.__log["die"].append(i['die'])
                            self.__log["key"].append(self.__key)
                        balanceTime = balanceTime - 1

                    eqpDate = eqpDate + 1
                    if eqpDate > maxDate:
                        maxDate = eqpDate

        #
        # x = open("devops.txt", "a")
        # for i in self.__list:
        #     x.write('\n%s' % i)
        # x.close()


    def update_flow(self):
        for key in list(self.__flow):
            if key < self.__startStep or key > self.__endStep:
                del self.__flow[key]

    def create_log(self):
        out = pd.DataFrame.from_dict(self.__log).drop_duplicates(subset=['step', 'eqp'], keep='last')
        #         out['datestamp'] = round_down(out['date'],1440)
        return out


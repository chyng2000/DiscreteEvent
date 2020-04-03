import pandas as pd


class WorkflowBuilder(object):
    def __init__(self, partnumberofnextlevel, workflowdetail, selectedpartnumber):
        self.pn = partnumberofnextlevel
        self.wf = workflowdetail
        self.check = selectedpartnumber

    def linkPartnumberToPartNumber(self):
        row0 = self.pn['Productname'].values.tolist()
        row1 = self.pn['NextLevelProductname'].values.tolist()

        pn_dict = {}
        for i in range(len(row0)):
            pn_dict.update({row0[i]: row1[i]})

        complete_list = []
        for i in range(len(row0)):
            complete_list.append([row0[i]])

        iteration = 4
        while iteration > 0:
            for i in range(len(complete_list)):
                current_pn = complete_list[i][-1]
                next_pn = "Na"
                try:
                    next_pn = pn_dict[current_pn]
                    complete_list[i].append(next_pn)
                except:
                    pass
            iteration = iteration - 1
        pn_list = []
        l = 0
        for n in complete_list:
            c = ["id{}".format(l)] * len(n)
            combine = list(map(list, zip(c, n)))
            for ln in combine:
                pn_list.append(ln)
            l = l + 1

        pn_df = pd.DataFrame(pn_list, columns=['flowID', 'Productname'])
        pn_df = pd.merge(pn_df, pn_df.groupby(['flowID']).flowID.agg('count').to_frame('c').reset_index()
                         , on=['flowID'], how='left').sort_values(['c'], ascending=[True])
        pn_df = pn_df.sort_index()

        return pn_df

    def linkWorkflowToWorkflow(self):
        workflow_name = self.wf[['Productname', 'Mfglevelname', 'Workflowname', 'Lmcproductname']].drop_duplicates(
            subset=['Productname', 'Workflowname'], keep='last')
        workflow_level_to_level = pd.merge(self.linkPartnumberToPartNumber(), workflow_name, on=['Productname'],
                                           how='left')
        df = workflow_level_to_level.copy().fillna({'Workflowname': '----'})
        df = df.groupby(['flowID'])['Workflowname'].apply('_'.join).reset_index()
        workflow_level_to_level = pd.merge(workflow_level_to_level, df, on=['flowID'], how='left')
        return workflow_level_to_level

    def linkWorkflowToSelected(self):
        wf2 = self.wf[['Mfglevelname', 'Productname', 'Stepsequence', 'Sb Specname', 'UPH_ForCapCheck', 'OEE',
                       'IE Cycle_Time(Hour)', 'Workflowname']]
        self.check = self.check.rename(columns={"partnumber": "Productname"})
        wf2 = pd.merge(self.check, wf2, on=['Productname'], how='left').drop_duplicates(
            subset=['Productname', 'Sb Specname'], keep='last')

        return wf2
from Package import chan_module as cm
from Package import chan_module2 as cm2
from Package import chan_prepocessing as pre
from Package import chan_method
from datetime import datetime
import pandas as pd
import timeit

# custom rounder
def round_down(num, divisor):
    return num - (num%divisor)


choice = ''
while choice != 'q':
    chan_method.clear()
    print("\n------ Please contact huan-yang.chan@lumileds.com if any queries ------")
    print("\nWhat would you like to do?")
    print("1 - Run model using daily forecast (with considering cycle time)")
    print("2 - Run model using daily forecast")
    print("3 - Run model using weekly forecast")
    print("4 - Run model using monthly forecast")
    print("5 - Data Pre-Processing")
    print("6 - Run manual work model using monthly forecast")
    print("q - Quit")

    choice = input("\nPlease enter your choice: ")

    if choice == '1':
        chan_method.clear()
        choice1 = ''
        while choice1 != 'b':
            print("\n------ Run model using daily forecast (with considering cycle time) ------")
            print("1 - Run")
            print("2 - Export as ODME")
            print("b - Back")
            choice1 = input("\nPlease enter your choice: ")
            if choice1 == '1':
                start = timeit.default_timer()
                chan_method.clear()
                d = chan_method.load_data()
                # Model - daily forecast using cycle time
                # Get info needed for iterator
                iterator_source = cm2.IteratorInfoCreator(d['RawDataForDailyCT'], d['Date'], d['Flow'], d['Off'])  # instantiate class IteratorInfoCreator
                dict_entity = iterator_source.createEntityDictionary()
                dict_flow = iterator_source.createFlowDictionary()
                dict_off = iterator_source.createOffDictionary()

                # Iterate all entity from dict_entity using iterator
                df_log = pd.DataFrame()
                for i in dict_entity:
                    obj1 = cm2.Iterator(i['product'], i['datestamp'], i['stepStart'], i['stepEnd'], i['qty'],
                                        dict_flow[i['product']].copy(), dict_off.copy(),
                                        i['key'])  # instantiate class Iterator
                    obj1.update_flow()
                    obj1.iterate(False)
                    df_log = df_log.append(obj1.create_log())
                #     print ("{} - Done".format(i['key']))
                df_log['datestamp'] = round_down(df_log['date'], 1440)
                df_log = pd.merge(df_log, d['Date'], on=['datestamp'], how='left')
                df_log['qty_unit'] = df_log['qty'] / df_log['die']

                # Create model and calculate
                EqpCalendar = cm.EqpCalendarCreator(d['Eqp'], d['Off'], d['Date'])  # instantiate class EqpCalendarCreator
                model = cm.ModelUsingDayCT(df_log, d['Flow'], d['Eqp'], d['Update'], EqpCalendar.getEqpDailyCalendar(),
                                           None)
                model.getDailyRequirement().to_csv("daily.csv", index=False)
                model.getWeeklyRequirement().to_csv("weekly.csv",index=False)
                stop = timeit.default_timer()
                dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print("Run model using daily forecast (with considering cycle time) successfully in {}s at {}".format(str(stop-start)[:4],dt_string))

            elif choice1 == '2':
                chan_method.clear()
                print("Module not ready yet")
            else:
                chan_method.clear()
                print("\nI don't understand that choice, please try again.\n")
        chan_method.clear()

    elif choice == '2':
        chan_method.clear()
        choice2 = ''
        while choice2 != 'b':
            print("\n------ Run model using daily forecast ------")
            print("1 - Run")
            print("2 - Export as ODME")
            print("b - Back")
            choice2 = input("\nPlease enter your choice: ")
            if choice2 == '1':
                start = timeit.default_timer()
                chan_method.clear()
                d = chan_method.load_data()
                # Model - daily forecast

                # Create model and calculate
                EqpCalendar = cm.EqpCalendarCreator(d['Eqp'], d['Off'],d['Date'])  # instantiate class EqpCalendarCreator
                model = cm.ModelUsingDay(d['DataForDaily'], d['Flow'], d['Eqp'], d['Update'], EqpCalendar.getEqpDailyCalendar(),EqpCalendar.getEqpWeeklyCalendar())
                model.getDailyRequirement().to_csv("daily.csv", index=False)
                model.getWeeklyRequirement().to_csv("weekly.csv",index=False)
                stop = timeit.default_timer()
                dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print("Run model using daily forecast successfully in {}s at {}".format(str(stop-start)[:4],dt_string))

            elif choice2 == '2':
                chan_method.clear()
                print("Module not ready yet")
            else:
                chan_method.clear()
                print("\nI don't understand that choice, please try again.\n")
        chan_method.clear()
    elif choice == '3':
        chan_method.clear()
        choice3 = ''
        while choice3 != 'b':
            print("\n------ Run model using weekly forecast ------")
            print("1 - Run")
            print("2 - Export as ODME")
            print("b - Back")
            choice3 = input("\nPlease enter your choice: ")
            if choice3 == '1':
                start = timeit.default_timer()
                chan_method.clear()
                d = chan_method.load_data()
                # Model - daily forecast

                # Create model and calculate
                EqpCalendar = cm.EqpCalendarCreator(d['Eqp'], d['Off'],d['Date'])  # instantiate class EqpCalendarCreator
                model = cm.ModelUsingWeek(d['DataForWeekly'], d['Flow'], d['Eqp'], d['Update'], EqpCalendar.getEqpDailyCalendar(),EqpCalendar.getEqpWeeklyCalendar())
                model.getWeeklyRequirement().to_csv("weekly.csv",index=False)
                model.getMonthlyRequirement().to_csv("monthly.csv", index=False)
                stop = timeit.default_timer()
                dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print("Run model using weekly forecast successfully in {}s at {}".format(str(stop-start)[:4],dt_string))

            elif choice3 == '2':
                chan_method.clear()
                print("Module not ready yet")
            else:
                chan_method.clear()
                print("\nI don't understand that choice, please try again.\n")
        chan_method.clear()
    elif choice == '4':
        chan_method.clear()
        choice4 = ''
        while choice4 != 'b':
            print("\n------ Run model using monthly forecast ------")
            print("1 - Run")
            print("2 - Export as ODME")
            print("b - Back")
            choice4 = input("\nPlease enter your choice: ")
            if choice4 == '1':
                start = timeit.default_timer()
                chan_method.clear()
                d = chan_method.load_data()
                # Model - daily forecast

                # Create model and calculate
                EqpCalendar = cm.EqpCalendarCreator(d['Eqp'], d['Off'],d['Date'])  # instantiate class EqpCalendarCreator
                model = cm.ModelUsingMonth(d['DataForMonthly'], d['Flow'], d['Eqp'], d['Update'], EqpCalendar.getEqpDailyCalendar(),EqpCalendar.getEqpMonthlyCalendar())
                # model.getDailyRequirement().to_csv("daily.csv", index=False)
                model.getWeeklyRequirement().to_csv("weekly.csv",index=False)
                model.getMonthlyRequirement().to_csv("monthly.csv", index=False)
                stop = timeit.default_timer()
                dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print("Run model using monthly forecast successfully in {}s at {}".format(str(stop-start)[:4],dt_string))

            elif choice4 == '2':
                chan_method.clear()
                print("Module not ready yet")
            else:
                chan_method.clear()
                print("\nI don't understand that choice, please try again.\n")
        chan_method.clear()

    elif choice == '5':
        chan_method.clear()
        choice5 = ''
        while choice5 != 'b':
            print("\n------ Data Preprocessing ------")
            print("1 - Workflow Builder")
            print("2 - Placeholder")
            print("b - Back")
            choice5 = input("\nPlease enter your choice: ")
            if choice5 == '1':
                start = timeit.default_timer()
                chan_method.clear()
                encoder = "cp1252"
                pn = pd.read_csv("./Input/NextPN.csv", usecols=[0, 1, 2], encoding=encoder).dropna(
                    how='any').drop_duplicates(subset=['Productname'], keep='last')
                wf = pd.read_csv("./Input/Workflow.csv", usecols=[2, 3, 4, 5, 6, 7, 10, 13, 17, 18, 22, 26],
                                 encoding=encoder)
                df_map = pd.read_csv("./Input/Map.csv", usecols=[1, 2], encoding=encoder).drop_duplicates(
                    subset=['partnumber'], keep='last')

                obj = pre.WorkflowBuilder(pn, wf, df_map)
                obj.linkWorkflowToSelected().to_csv("workflowselected.csv",index=False)

                stop = timeit.default_timer()
                dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print("Run Workflow Builder successfully in {}s at {}".format(str(stop - start)[:4],dt_string))

            elif choice5 == '2':
                chan_method.clear()
                print("Module not ready yet")
            else:
                chan_method.clear()
                print("\nI don't understand that choice, please try again.\n")
        chan_method.clear()

    elif choice == '6':
        chan_method.clear()
        choice6 = ''
        while choice6 != 'b':
            print("\n------ Run manual work model using monthly forecast ------")
            print("1 - Run")
            print("b - Back")
            choice6 = input("\nPlease enter your choice: ")
            if choice6 == '1':
                start = timeit.default_timer()
                chan_method.clear()
                df = chan_method.load_data3()

                # Create model and calculate
                wb_model = cm.manual_work_model(df['forecast'], df['calendar'], df['shutdown'], df['uom'], df['process'])
                report = wb_model.get_report()
                report['manhour'].to_csv("manhour.csv",index=False)
                report['qtychecked'].to_csv("qtychecked.csv", index=False)
                report['stepchecked'].to_csv("stepchecked.csv", index=False)

                stop = timeit.default_timer()
                dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                print("Run model using monthly forecast successfully in {}s at {}".format(str(stop-start)[:4],dt_string))

            else:
                chan_method.clear()
                print("\nI don't understand that choice, please try again.\n")
        chan_method.clear()


    elif choice == 'q':
        print("\nThanks for using. See you later.\n")
    else:
        chan_method.clear()
        print("\nI don't understand that choice, please try again.\n")

# Print a message that we are all finished.
print("Bye Bye")
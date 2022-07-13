import wrds
import pandas as pd


db = wrds.Connection(wrds_username='jacernes')
db.create_pgpass_file()

#help(db.raw_sql)

#data = db.raw_sql("select * from trace.trace_enhanced where TRD_EXCTN_DT >= '07/01/2002' and TRD_EXCTN_DT < '08/01/2002'", date_cols=['date'])

for i in range(2002,2022):

    if i == 2002:

        for f in range (7,13):


            #Set current month string
            if f < 10:
                monthStr = "0" + str(f)

            else:
                monthStr = str(f)


            if f < 9:
                nextMonthStr = "0" + str(f+1)

            elif f ==12:
                nextMonthStr = "01"

            else:
                nextMonthStr = str(f+1)



            #Set year string
            if f == 12:
                yearStr = str(i+1)
            
            else:
                yearStr = str(i)

            data = db.raw_sql("select * from trace.trace_enhanced where TRD_EXCTN_DT >= '" + monthStr + "/01/" + str(i) + "' and TRD_EXCTN_DT < '" + nextMonthStr + "/01/" + yearStr + "'", date_cols=['date'])
            print("query done")

            df = pd.DataFrame(data)
            df.to_csv(str(i) + monthStr + ".csv",index=False)


    

    elif i == 2021:

        for f in range (1,10):


            #Set current month string
            if f < 10:
                monthStr = "0" + str(f)

            else:
                monthStr = str(f)


            if f < 9:
                nextMonthStr = "0" + str(f+1)

            elif f ==12:
                nextMonthStr = "01"

            else:
                nextMonthStr = str(f+1)



            #Set year string
            if f == 12:
                yearStr = str(i+1)
            
            else:
                yearStr = str(i)

            data = db.raw_sql("select * from trace.trace_enhanced where TRD_EXCTN_DT >= '" + monthStr + "/01/" + str(i) + "' and TRD_EXCTN_DT < '" + nextMonthStr + "/01/" + yearStr + "'", date_cols=['date'])
            print("query done")

            df = pd.DataFrame(data)
            df.to_csv(str(i) + monthStr + ".csv",index=False)


    else:
        for f in range(1,13):


            #Set current month string
            if f < 10:
                monthStr = "0" + str(f)

            else:
                monthStr = str(f)


            if f < 9:
                nextMonthStr = "0" + str(f+1)

            elif f ==12:
                nextMonthStr = "01"

            else:
                nextMonthStr = str(f+1)



            #Set year string
            if f == 12:
                yearStr = str(i+1)
            
            else:
                yearStr = str(i)

            data = db.raw_sql("select * from trace.trace_enhanced where TRD_EXCTN_DT >= '" + monthStr + "/01/" + str(i) + "' and TRD_EXCTN_DT < '" + nextMonthStr + "/01/" + yearStr + "'", date_cols=['date'])
            print("query done")

            df = pd.DataFrame(data)
            df.to_csv(str(i) + monthStr + ".csv",index=False)



#df = pd.DataFrame(data)
#df.to_csv("testCSV.csv",index=False)

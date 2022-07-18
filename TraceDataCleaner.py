
import pandas as pd
import numpy as np

#Utility function to help create file names
def prependNum(number):

    if number < 10:
        return "0"+str(number)

    else:
        return str(number)


#Utility function to help create files names
def createFileName(year,month):

    return str(year) + prependNum(month) + ".csv"


#Function which deals with the cancelations before 2012
def cleanCancelationsPre2012(dataframe):

    #Copy all cancellations into a new dataframe, then remove cancelations from the existing dataframe
    #cancellations is the equivalent of temp_delete
    cancellations = dataframe[(dataframe["trc_st"] == "C")].copy()
    cancellations = pd.concat([cancellations, dataframe[(dataframe["trc_st"] == "W")]])
    dataframe.drop(dataframe[dataframe["trc_st"] == "C"].index , inplace=True)

    #Obtain list of all possible days
    list_of_days = dataframe.drop_duplicates(subset="trd_rpt_dt")["trd_rpt_dt"].to_numpy()

    #Loop through every date in the data
    for day in list_of_days:

        #Create a temporary dataframe of all transactions on a given day and all cancelations on a given day
        #This is to help with the efficiency of the code, it will result in less comparisons overall
        temp_dataframe = dataframe[dataframe["trd_rpt_dt"] == day]
        temp_cancellations = cancellations[cancellations["trd_rpt_dt"] == day]

        #Loop through all transactions of a given day
        #for index1,row1 in temp_dataframe.iterrows():

            #Loop through all cancellations of the same day day
            #for index2,row2 in temp_cancellations:

                #if temp_dataframe["msg_seq_nb"][index1] == temp_cancellations["orig_msg_seq_nb"][index2]:
                    #dataframe.drop(temp_dataframe["msg_seq_nb"][index1])

        #Iterate through every cancellation for a given day
        for index2,row2 in temp_cancellations.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            dataframe.drop(temp_dataframe[temp_dataframe["msg_seq_nb"] == temp_cancellations["orig_msg_seq_nb"][index2]].index )

    #Returns the newly cleaned dataframe
    return dataframe



def cleanCancelationsPost2012(dataframe):
    

    cancellations = dataframe[ (dataframe["trc_st"] == "X") | (dataframe["trc_st"] == "C")].copy()

    dataframe.drop(dataframe[(dataframe["trc_st"] == "X") | (dataframe["trc_st"] == "C")].index,inplace=True)

    #Obtain list of all possible days
    list_of_days = dataframe.drop_duplicates(subset="trd_rpt_dt")["trd_rpt_dt"].to_numpy()

    #Loop through every date in the data
    for day in list_of_days:

        
        #Create a temporary dataframe of all transactions on a given day and all cancelations on a given day
        #This is to help with the efficiency of the code, it will result in less comparisons overall
        temp_dataframe = dataframe[dataframe["trd_rpt_dt"] == day]
        temp_cancellations = cancellations[cancellations["trd_rpt_dt"] == day]

        #Iterate through every cancellation for a given day
        for index,row in temp_cancellations.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            dataframe.drop(temp_dataframe[ temp_dataframe['cusip_id']==row['cusip_id'] &  temp_dataframe['entrd_vol_qt']==row['entrd_vol_qt'] & temp_dataframe['rptd_pr']==row['rptd_pr'] & temp_dataframe['trd_exctn_dt']==row['trd_exctn_dt'] & temp_dataframe['trd_exctn_tm']==row['trd_exctn_tm'] & temp_dataframe['rpt_side_cd']==row['rpt_side_cd'] &  temp_dataframe['cntra_mp_id']==row['cntra_mp_id'] & temp_dataframe['msg_seq_nb']==row['msg_seq_nb'] ].index )

    return dataframe



#Function used to remove the reversals in the data before 2012
def cleanReversalsPre2012(dataframe):

    #Seperate reversals from the rest of the transactions
    reversals = dataframe[dataframe["asof_cd"] == "R"].copy()
    dataframe.drop(dataframe[dataframe["asof_cd"] == "R"].index,inplace=True)

    #Remove duplicates from reversals,note this step might not be needed
    reversals.drop_duplicates(subset=["trd_exctn_dt","cusip_id","trd_exctn_tm","rptd_pr","entrd_vol_qt","rpt_side_cd","cntra_mp_id","trd_rpt_dt","trd_rpt_tm","msg_seq_nb"],keep="first",inplace=True)

    #Loop through every reversal
    for index,row in reversals.iterrows():

        #Pull all transactions that match the current reversal
        matched_transactions = dataframe[(dataframe["trd_exctn_dt"] < row["trd_rpt_dt"]) & (dataframe["trd_exctn_dt"] == row["trd_exctn_dt"]) & (dataframe["cusip_id"] == row["cusip_id"]) & (dataframe["trd_exctn_tm"] == row["trd_exctn_tm"]) & (dataframe["rptd_pr"] == row["rptd_pr"]) & (dataframe["entrd_vol_qt"] == row["entrd_vol_qt"]) & (dataframe["rpt_side_cd"] == row["rpt_side_cd"]) & (dataframe["cntra_mp_id"] == row["cntra_mp_id"])].copy()
        
        #If for some reason there's more than one that matches, keep only the first one
        matched_transactions.drop_duplicates(subset=["trd_exctn_dt","bond_sym_id","trd_exctn_tm","rptd_pr","entrd_vol_qt"],keep="first",inplace=True)

        #Drop the index of the selected transaction
        dataframe.drop(matched_transactions.index)


    #Return the new dataframe with reversals removed
    return dataframe 



def cleanReversalsPost2012(dataframe):

    reversals = dataframe[dataframe["trc_st"] == "Y"].copy()
    dataframe.drop(dataframe[dataframe["trc_st"] == "Y"].index,inplace=True)

    #Obtain list of all possible days
    list_of_days = dataframe.drop_duplicates(subset="trd_rpt_dt")["trd_rpt_dt"].to_numpy()

        

    #Iterate through every cancellation for a given day
    for index,row in reversals.iterrows():

        #Remove any rows where the msg_seq_nb matches a given cancellation
        dataframe.drop(dataframe[ dataframe['cusip_id']==row['cusip_id'] &  dataframe['entrd_vol_qt']==row['entrd_vol_qt'] & dataframe['rptd_pr']==row['rptd_pr'] & dataframe['trd_exctn_dt']==row['trd_exctn_dt'] & dataframe['trd_exctn_tm']==row['trd_exctn_tm'] & dataframe['rpt_side_cd']==row['rpt_side_cd'] &  dataframe['cntra_mp_id']==row['cntra_mp_id'] & dataframe['msg_seq_nb']==row['orig_msg_seq_nb'] ].index )

    return dataframe


def cleanMonth(cur_month,next_month=False):

    cur_month.dropna(subset=["cusip_id"],inplace=True)

    if(next_month == False):

        if(cur_month.head(1)["trd_rpt_dt"] < "2012-02-06"):
            cleanCancelationsPre2012(cur_month)
            cleanReversalsPre2012(cur_month)

        else:
            cleanCancelationsPost2012(cur_month)
            cleanReversalsPost2012(cur_month)


    else:

        if(cur_month.head(1)["trd_rpt_dt"] < "2012-02-06"):
            cleanCancelationsPre2012(cur_month)
            cleanReversalsPre2012(cur_month,next_month = next_month)

        else:
            cleanCancelationsPost2012(cur_month)
            cleanReversalsPost2012(cur_month, next_month)




#Main block of code which imports the data, and calls the functions nessecary for cleaning it

'''
#Prompt user for filename, open it
fileName = input("Enter the file or path to the file containing the TRACE data: ")
trace_data = pd.read_csv(fileName, low_memory=False)


#Remove all entries with no cusip_id
trace_data.dropna(subset=["cusip_id"],inplace=True)
#trace_data.drop(trace_data[trace_data["cusip_id"] == ""].index,inplace=True)


pre2012 = trace_data[trace_data["trd_rpt_dt"] < "2012-02-06"].copy()
post2012 = trace_data[trace_data["trd_rpt_dt"] >= "2012-02-06"].copy()

print("Before any changes")
print(pre2012)
print(post2012)


cleanCancelationsPre2012(pre2012)
cleanCancelationsPost2012(post2012)


print("After cancellations are removed")
print(pre2012)
print(post2012)

cleanReversalsPre2012(pre2012)
cleanReversalsPost2012(post2012)


print("After reversals are removed")
print(pre2012)
print(post2012)
'''


fileNames = []

for i in range(2002,2022):
    

    if i == 2002:

        for f in range(7,13):

            fileNames.append(createFileName(i,f))

    if i == 2021:

        for f in range(1,10):

            fileNames.append(createFileName(i,f))

    else:

        for f in range(1,13):

            fileNames.append(createFileName(i,f))


length = len(fileName)

for i in range(length):

    if i == length - 1:

        cur_month = pd.read_csv(fileNames[i], low_memory=False)
        cleanMonth(cur_month)
        

    else:

        cur_month = pd.read_csv(fileNames[i], low_memory=False)
        next_month = pd.read_csv(fileNames[i+1],low_memory = False)
        cleanMonth(cur_month,next_month=next_month)

        










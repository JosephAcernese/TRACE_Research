
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

        #Iterate through every cancellation for a given day
        for index,row in temp_cancellations.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            dataframe.drop(temp_dataframe[temp_dataframe["msg_seq_nb"] == row["orig_msg_seq_nb"]].index,inplace=True )

    #Returns the newly cleaned dataframe
    return dataframe



def cleanCancelationsPost2012(dataframe):
    

    cancellations = dataframe[ (dataframe["trc_st"] == "X") | (dataframe["trc_st"] == "C")].copy()

    dataframe.drop(dataframe[(dataframe["trc_st"] == "X") | (dataframe["trc_st"] == "C")].index,inplace=True)

    #Obtain list of all possible days
    list_of_days = dataframe.drop_duplicates(subset="trd_exctn_dt")["trd_exctn_dt"].to_numpy()

    #Loop through every date in the data
    for day in list_of_days:

        
        #Create a temporary dataframe of all transactions on a given day and all cancelations on a given day
        #This is to help with the efficiency of the code, Dit will result in less comparisons overall
        temp_dataframe = dataframe[dataframe["trd_exctn_dt"] == day]
        temp_cancellations = cancellations[cancellations["trd_exctn_dt"] == day]

        #Iterate through every cancellation for a given day
        for index,row in temp_cancellations.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            dataframe.drop(temp_dataframe[ temp_dataframe['cusip_id']==row['cusip_id'] &  temp_dataframe['entrd_vol_qt']==row['entrd_vol_qt'] & temp_dataframe['rptd_pr']==row['rptd_pr'] & temp_dataframe['trd_exctn_tm']==row['trd_exctn_tm'] & temp_dataframe['rpt_side_cd']==row['rpt_side_cd'] &  temp_dataframe['cntra_mp_id']==row['cntra_mp_id'] & temp_dataframe['msg_seq_nb']==row['msg_seq_nb'] ].index )
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
        matched_transactions = dataframe[(dataframe["trd_exctn_dt"] < row["trd_rpt_dt"]) & (dataframe["trd_exctn_dt"] == row["trd_exctn_dt"]) & (dataframe["cusip_id"] == row["cusip_id"]) & (dataframe["trd_exctn_tm"] == row["trd_exctn_tm"]) & (dataframe["rptd_pr"] == row["rptd_pr"]) & (dataframe["entrd_vol_qt"] == row["entrd_vol_qt"]) & (dataframe["rpt_side_cd"] == row["rpt_side_cd"]) & (dataframe["cntra_mp_id"] == row["cntra_mp_id"])]
        
        #If for some reason there's more than one that matches, keep only the first one
        matched_transactions = matched_transactions.drop_duplicates(subset=["trd_exctn_dt","bond_sym_id","trd_exctn_tm","rptd_pr","entrd_vol_qt"],keep="first")

        #Drop the index of the selected transaction
        dataframe.drop(matched_transactions.index)


    #Return the new dataframe with reversals removed
    return dataframe 



def cleanReversalsPost2012(dataframe):

    reversals = dataframe[dataframe["trc_st"] == "Y"].copy()
    dataframe.drop(dataframe[dataframe["trc_st"] == "Y"].index,inplace=True)

    #Obtain list of all possible days
    list_of_days = dataframe.drop_duplicates(subset="trd_exctn_dt")["trd_exctn_dt"].to_numpy()


    #Iterate through every day
    for day in list_of_days:

        #Create temp dataframes to help with efficiency
        temp_dataframe = dataframe[dataframe["trd_exctn_dt"] == day]
        temp_reversals = reversals[reversals["trd_exctn_dt"] == day]

        #Iterate through every cancellation for a given day
        for index,row in temp_reversals.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            dataframe.drop(temp_dataframe[ temp_dataframe['cusip_id']==row['cusip_id'] &  temp_dataframe['entrd_vol_qt']==row['entrd_vol_qt'] & temp_dataframe['rptd_pr']==row['rptd_pr'] & temp_dataframe['trd_exctn_tm']==row['trd_exctn_tm'] & temp_dataframe['rpt_side_cd']==row['rpt_side_cd'] &  temp_dataframe['cntra_mp_id']==row['cntra_mp_id'] & temp_dataframe['msg_seq_nb']==row['orig_msg_seq_nb'] ].index, inplace=True )

    return dataframe


def cleanMonth(cur_month):

    cur_month.dropna(subset=["cusip_id"],inplace=True)

    pre2012 = cur_month[cur_month["trd_rpt_dt"] < "2012-02-06"]
    post2012 = cur_month[cur_month["trd_rpt_dt"] >= "2012-02-06"]

    if len(pre2012.index) > 0:
        pre2012 = cleanCancelationsPre2012(pre2012)
        pre2012 = cleanReversalsPre2012(pre2012)
    
    if len(post2012.index) > 0:
        post2012 = cleanCancelationsPost2012(post2012)
        post2012 = cleanReversalsPost2012(post2012)


    cur_month = pd.concat([pre2012,post2012])
    return cur_month



#Main block of code which imports the data, and calls the functions nessecary for cleaning it


'''
fileNames = []

#Loop to generate file names
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


length = len(fileNames)

for i in range(length):

    cur_month = pd.read_csv(fileNames[i], low_memory=False)
    cleanMonth(cur_month)
    print(fileNames[i] + " has been cleaned")
    cur_month.to_csv("./CleanTraceData/clean_"+fileNames[i])
    print(fileNames[i] + " has been saved")
'''

#This block of code is meant for my own personal debugging
cur_month = pd.read_csv("./TestFile/TestData.csv", low_memory=False)
cur_month = cleanMonth(cur_month)
print(cur_month)

cur_month = pd.read_csv("./TestFile/TestData2.csv", low_memory=False)
cur_month = cleanMonth(cur_month)
print(cur_month)
 





        










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
def cleanCancelationsPre2012():

    #Copy all cancellations into a new dataframe, then remove cancelations from the existing dataframe
    #cancellations is the equivalent of temp_delete
    cancellations = cur_month[ (cur_month["trd_rpt_dt"] < "2012-02-06") & ((cur_month["trc_st"] == "C") | (cur_month["trc_st"] == "W"))]
    cur_month.drop(cancellations.index , inplace=True)

    #Obtain list of all possible days
    list_of_days = cancellations.drop_duplicates(subset="trd_rpt_dt")["trd_rpt_dt"].to_numpy()

    #Loop through every date in the data
    for day in list_of_days:

        #Create a temporary dataframe of all transactions on a given day and all cancelations on a given day
        #This is to help with the efficiency of the code, it will result in less comparisons overall
        temp_dataframe = cur_month[cur_month["trd_rpt_dt"] == day]
        temp_cancellations = cancellations[cancellations["trd_rpt_dt"] == day]

        #Iterate through every cancellation for a given day
        for index,row in temp_cancellations.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            cur_month.drop(temp_dataframe[temp_dataframe["msg_seq_nb"] == row["orig_msg_seq_nb"]].index,inplace=True )


def cleanCancelationsPost2012():
    

    cancellations = cur_month[ (cur_month["trd_rpt_dt"] >= "2012-02-06") & ((cur_month["trc_st"] == "X") | (cur_month["trc_st"] == "C"))].copy()

    cur_month.drop(cancellations.index,inplace=True)

    #Obtain list of all possible days
    list_of_days = cancellations.drop_duplicates(subset="trd_exctn_dt")["trd_exctn_dt"].to_numpy()

    #Loop through every date in the data
    for day in list_of_days:

        
        #Create a temporary dataframe of all transactions on a given day and all cancelations on a given day
        #This is to help with the efficiency of the code, Dit will result in less comparisons overall
        temp_dataframe = cur_month[cur_month["trd_exctn_dt"] == day]
        temp_cancellations = cancellations[cancellations["trd_exctn_dt"] == day]

        #Iterate through every cancellation for a given day
        for index,row in temp_cancellations.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            cur_month.drop(temp_dataframe[ temp_dataframe['cusip_id']==row['cusip_id'] &  temp_dataframe['entrd_vol_qt']==row['entrd_vol_qt'] & temp_dataframe['rptd_pr']==row['rptd_pr'] & temp_dataframe['trd_exctn_tm']==row['trd_exctn_tm'] & temp_dataframe['rpt_side_cd']==row['rpt_side_cd'] &  temp_dataframe['cntra_mp_id']==row['cntra_mp_id'] & temp_dataframe['msg_seq_nb']==row['msg_seq_nb'] ].index, inplace=True )



#Function used to remove the reversals in the data before 2012
def cleanReversalsPre2012():

    #Seperate reversals from the rest of the transactions
    reversals = cur_month[ (cur_month["trd_rpt_dt"] >= "2012-02-06") & (cur_month["asof_cd"] == "R")]
    cur_month.drop(reversals.index)

    #Remove duplicates from reversals,note this step might not be needed
    reversals.drop_duplicates(subset=["trd_exctn_dt","cusip_id","trd_exctn_tm","rptd_pr","entrd_vol_qt","rpt_side_cd","cntra_mp_id","trd_rpt_dt","trd_rpt_tm","msg_seq_nb"],keep="first",inplace=True)

    #Obtain list of all possible days
    list_of_days = reversals.drop_duplicates(subset="trd_exctn_dt")["trd_exctn_dt"].to_numpy()

    for day in list_of_days:

        temp_reversals = reversals[reversals["trd_exctn_dt"] == day]
        temp_dataframe = cur_month[cur_month["trd_exctn_dt"] == day]

        #Loop through every reversal
        for index,row in temp_reversals.iterrows():

            #Pull all transactions that match the current reversal
            matched_transactions = temp_dataframe[(temp_dataframe["trd_exctn_dt"] < row["trd_rpt_dt"]) & (temp_dataframe["cusip_id"] == row["cusip_id"]) & (temp_dataframe["trd_exctn_tm"] == row["trd_exctn_tm"]) & (temp_dataframe["rptd_pr"] == row["rptd_pr"]) & (temp_dataframe["entrd_vol_qt"] == row["entrd_vol_qt"]) & (temp_dataframe["rpt_side_cd"] == row["rpt_side_cd"]) & (temp_dataframe["cntra_mp_id"] == row["cntra_mp_id"])]
            
            #If for some reason there's more than one that matches, keep only the first one
            matched_transactions = matched_transactions.drop_duplicates(subset=["trd_exctn_dt","bond_sym_id","trd_exctn_tm","rptd_pr","entrd_vol_qt"],keep="first")

            #Drop the index of the selected transaction
            cur_month.drop(matched_transactions.index,inplace=True)



def cleanReversalsPost2012():

    reversals = cur_month[(cur_month["trc_st"] == "Y") & (cur_month["trd_rpt_dt"] >= "2012-02-06") ]
    cur_month.drop(reversals.index,inplace=True)

    #Obtain list of all possible days
    list_of_days = reversals.drop_duplicates(subset="trd_exctn_dt")["trd_exctn_dt"].to_numpy()


    #Iterate through every day
    for day in list_of_days:

        #Create temp dataframes to help with efficiency
        temp_dataframe = cur_month[cur_month["trd_exctn_dt"] == day]
        temp_reversals = reversals[reversals["trd_exctn_dt"] == day]

        #Iterate through every cancellation for a given day
        for index,row in temp_reversals.iterrows():

            #Remove any rows where the msg_seq_nb matches a given cancellation
            cur_month.drop(temp_dataframe[ temp_dataframe['cusip_id']==row['cusip_id'] &  temp_dataframe['entrd_vol_qt']==row['entrd_vol_qt'] & temp_dataframe['rptd_pr']==row['rptd_pr'] & temp_dataframe['trd_exctn_tm']==row['trd_exctn_tm'] & temp_dataframe['rpt_side_cd']==row['rpt_side_cd'] &  temp_dataframe['cntra_mp_id']==row['cntra_mp_id'] & temp_dataframe['msg_seq_nb']==row['orig_msg_seq_nb'] ].index, inplace=True )


def cleanMonth():

    cur_month.dropna(subset=["cusip_id"],inplace=True)

    cleanCancelationsPre2012()
    cleanReversalsPre2012()
    
    cleanCancelationsPost2012()
    cleanReversalsPost2012()

'''
#Main block of code which imports the data, and calls the functions nessecary for cleaning it
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
    cleanMonth()
    print(fileNames[i] + " has been cleaned")
    cur_month.to_csv("./CleanTraceData/clean_"+fileNames[i], index=False)
    print(fileNames[i] + " has been saved")
    
'''

#This block of code is meant for my own personal debugging
cur_month = pd.read_csv("./TestFile/TestData.csv", low_memory=False)


print("FOR FIRST FILE BEFORE CLEAN")

print("Total observations: " + str(len(cur_month.index)))
print("Total reversals: " + str(len(cur_month[cur_month["asof_cd"] == "R"].index)))
print("Total cancellations: " + str(len(cur_month[(cur_month["trc_st"] == "C") | (cur_month["trc_st"] == "W")].index)))

cleanMonth()

cur_month.to_csv("./TestFile/Clean_TestData.csv",index=False)



print("FOR FIRST FILE AFTER CLEAN")

print("Total observations: " + str(len(cur_month.index)))
print("Total reversals: " + str(len(cur_month[cur_month["asof_cd"] == "R"].index)))
print("Total cancellations: " + str(len(cur_month[(cur_month["trc_st"] == "C") | (cur_month["trc_st"] == "W")].index)))


cur_month = pd.read_csv("./TestFile/TestData2.csv", low_memory=False)
cur_month.to_csv("./TestFile/Clean_TestData2.csv",index=False)

cleanMonth()





        










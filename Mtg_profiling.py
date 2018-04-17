cd /v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/PreAcxiom/InputTables/Test

/ms/dist/python/PROJ/core/3.4.4-2-64/bin/python

import ms.version
ms.version.addpkg('pyxml', '0.8.4')

# import numpy
import ms.version
ms.version.addpkg('numpy', '1.11.1-ms2')
import numpy as np

# import pandas
import ms.version
ms.version.addpkg('pandas', '0.19.2')
ms.version.addpkg('dateutil', '2.6.0')
ms.version.addpkg('pytz', '2016.6.1')
ms.version.addpkg('six', '1.9.0')
ms.version.addpkg('pandas', '0.19.2')
import six,pytz,dateutil
import pandas as pd

from datetime import datetime
from dateutil.parser import parse


###########################################################################
# read sample file for test
testdata=pd.read_csv("FalconMortgageSample.csv",delimiter="|")

# select all data with condition on one column 
temp=testdata.loc[testdata['CurrentAPR']>4]
# distinct count of loans
temp.NUM_LOAN.nunique()

# write result to csv file
temp.to_csv('result.csv',sep=',')


############################################################################



#write result to a file
/ms/dist/python/PROJ/core/3.4.4-2-64/bin/python python1.py > result.txt



############################################################################
# run on all internal mortgage data
data=pd.read_csv("FalconMortgageDataRaw_v1.1.CSV",delimiter="|",low_memory=False)

# select all records with DLQ, DLQ_30, DLQ_60
DLQ=data.loc[data['DLQ_DAYS']>0]
DLQ_30=data.loc[data['DLQ_DAYS']>30]
DLQ_60=data.loc[data['DLQ_DAYS']>60]

# write result to a csv file
DLQ.to_csv('DLQ_records.csv',sep=',')

#distinct count of loans with DLQ days > 0
DLQ.NUM_LOAN.nunique()
#2087 3.5% of total

#distinct count of loans with DLQ days > 0
DLQ_30.NUM_LOAN.nunique()
#880 1.5% of total

#distinct count of loans with DLQ days > 0
DLQ_60.NUM_LOAN.nunique()
#191 0.3% of total

#total distinct count of loans 
data.NUM_LOAN.nunique()
#60373





################################################################
#second try 

# load dataset 
data=pd.read_csv("FalconMortgageDataRaw_v1.1.CSV",delimiter="|",low_memory=False)

data_backup=data

# fill in missing values
data['NME_LEAD_STATUS_TYP']=data.groupby(['NUM_LOAN'])['NME_LEAD_STATUS_TYP'].ffill()
data['NME_LEAD_STATUS_TYP']=data.groupby(['NUM_LOAN'])['NME_LEAD_STATUS_TYP'].bfill()
data['LoanPurpose']=data.groupby(['NUM_LOAN'])['LoanPurpose'].ffill()
data['LoanPurpose']=data.groupby(['NUM_LOAN'])['LoanPurpose'].bfill()
data['PropertyType']=data.groupby(['NUM_LOAN'])['PropertyType'].ffill()
data['PropertyType']=data.groupby(['NUM_LOAN'])['PropertyType'].bfill()
data['LoanMaturityDate']=data.groupby(['NUM_LOAN'])['LoanMaturityDate'].ffill()
data['LoanMaturityDate']=data.groupby(['NUM_LOAN'])['LoanMaturityDate'].bfill()
data['v_PRODUCT']=data.groupby(['NUM_LOAN'])['v_PRODUCT'].ffill()
data['v_PRODUCT']=data.groupby(['NUM_LOAN'])['v_PRODUCT'].bfill()
data['IND_HNW']=data.groupby(['NUM_LOAN'])['IND_HNW'].ffill()
data['IND_HNW']=data.groupby(['NUM_LOAN'])['IND_HNW'].bfill()
data['OrigAPR']=data.groupby(['NUM_LOAN'])['OrigAPR'].ffill()
data['OrigAPR']=data.groupby(['NUM_LOAN'])['OrigAPR'].bfill()
data['OrigNoteRate']=data.groupby(['NUM_LOAN'])['OrigNoteRate'].ffill()
data['OrigNoteRate']=data.groupby(['NUM_LOAN'])['OrigNoteRate'].bfill()
data['OrigAppraisalValue']=data.groupby(['NUM_LOAN'])['OrigAppraisalValue'].ffill()
data['OrigAppraisalValue']=data.groupby(['NUM_LOAN'])['OrigAppraisalValue'].bfill()

# create new variable for analysis
# conforming vs jumbo
data['comforming'] = ['Comforming' if x <= 424100 else 'Jumbo' for x in data['BaseLoanAmount']]

# loan amount bucket
data['Loanamt_Bucket'] = np.where(data.BaseLoanAmount<100000, 'Less than 100k', np.where(data.BaseLoanAmount<300000,'100k - 300k', np.where(data.BaseLoanAmount<500000,'300k - 500k', np.where(data.BaseLoanAmount<700000,'500k - 700k', np.where(data.BaseLoanAmount<1000000,'700k - 1MM', np.where(data.BaseLoanAmount<3000000,'1MM - 3MM', 'more than 3MM'))))))

# tenure
data['DT2_BUSINESS'] =  pd.to_datetime(data['DT2_BUSINESS'], format='%m/%d/%Y')
data['LoanFundedDate'] =  pd.to_datetime(data['LoanFundedDate'], format='%m/%d/%Y')

data['tenure'] = (data.DT2_BUSINESS - data.LoanFundedDate)/ np.timedelta64(1, 'M')
data.tenure=data.tenure.round()

# DLQ 30 indicator
data['DLQ_30'] = [1 if x >= 30 else 0 for x in data['DLQ_DAYS']]

# DLQ 60 indicator
data['DLQ_60'] = [1 if x >= 60 else 0 for x in data['DLQ_DAYS']]

# DLQ 90 indicator
data['DLQ_90'] = [1 if x >= 90 else 0 for x in data['DLQ_DAYS']]

# DLQ 120 indicator
data['DLQ_120'] = [1 if x >= 120 else 0 for x in data['DLQ_DAYS']]


# prepayment indicator 
data['prepayment'] = np.where((data['LoanPaidInFullDate'] is not None)&(data['LoanPaidInFullDate']<data['LoanMaturityDate']),1,0)

# FICO outlier indicator
data['outlier_FICO'] = np.where((data['OrigMedianCreditScore'] <300)|(data['OrigMedianCreditScore']>850),1,0)

# create bin for FICO score
data['FICO_bin'] = np.where(data.OrigMedianCreditScore<700, 'Less than 700', np.where(data.OrigMedianCreditScore<737,'700-737', np.where(data.OrigMedianCreditScore<771,'737-771', np.where(data.OrigMedianCreditScore<800,'771-800', np.where(data.OrigMedianCreditScore<850,'800-850', np.where(data.OrigMedianCreditScore<850,'700 - 850', 'more than 850'))))))


# create temp columns for loan age 
data['today'] = pd.to_datetime('11/01/2017', format='%m/%d/%Y')
data['LoanPaidInFullDate_2']=data["LoanPaidInFullDate"].fillna('12/31/2099')
data['LoanCancellationDate_2']=data["LoanCancellationDate"].fillna('12/31/2099')

data['LoanPaidInFullDate_2'] =  pd.to_datetime(data['LoanPaidInFullDate_2'], format='%m/%d/%Y')
data['LoanCancellationDate_2'] =  pd.to_datetime(data['LoanCancellationDate_2'], format='%m/%d/%Y')

data['min_date'] = data[['today','LoanPaidInFullDate_2','LoanCancellationDate_2']].min(axis=1)
data['loan_age'] = round((data.min_date - data.LoanFundedDate)/ np.timedelta64(1, 'M'))
data.loan_age=data.loan_age.round()

# survival months 
data['min_date2'] = data[['DT2_BUSINESS','LoanPaidInFullDate_2','LoanCancellationDate_2']].min(axis=1)
data['survival_month'] = round((data.min_date2 - data.LoanFundedDate)/ np.timedelta64(1, 'M'))

data['survival_month_ind0'] = [1 if x >= 60 else 0 for x in data['DLQ_DAYS']]
data['survival_month_ind'] = data.groupby(['NUM_LOAN'])['survival_month_ind0'].apply(lambda x: x.cumsum())



# data[["DT2_BUSINESS","today","LoanPaidInFullDate","LoanCancellationDate","loan_age"]].sort_values(["DT2_BUSINESS"], ascending=True)
# create loan age 
data=data.drop(['today','LoanPaidInFullDate_2','LoanCancellationDate_2','min_date'],1)



# output new dataset
data.to_csv('Internal_Mtg_Clean.csv',sep='|')




# create subset 
DLQ_30=data.loc[data['DLQ_30']==1]
DLQ_60=data.loc[data['DLQ_60']==1]
DLQ_90=data.loc[data['DLQ_90']==1]
DLQ_120=data.loc[data['DLQ_120']==1]
Prepay=data.loc[data['prepayment']==1]


# dedup for each group 
DLQ_30_dedup=DLQ_30.sort('DT2_BUSINESS').drop_duplicates(subset='NUM_LOAN', keep="first")
DLQ_60_dedup=DLQ_60.sort('DT2_BUSINESS').drop_duplicates(subset='NUM_LOAN', keep="first")
DLQ_90_dedup=DLQ_90.sort('DT2_BUSINESS').drop_duplicates(subset='NUM_LOAN', keep="first")
DLQ_120_dedup=DLQ_120.sort('DT2_BUSINESS').drop_duplicates(subset='NUM_LOAN', keep="first")
Prepay_dedup=Prepay.sort('DT2_BUSINESS').drop_duplicates(subset='NUM_LOAN', keep="first")
all_loan_dedup=data.sort('DT2_BUSINESS').drop_duplicates(subset='NUM_LOAN', keep="first")


# output result to csv 
DLQ_30_dedup.to_csv('DLQ_30.csv',sep=',')
DLQ_60_dedup.to_csv('DLQ_60.csv',sep=',')
DLQ_90_dedup.to_csv('DLQ_90.csv',sep=',')
DLQ_120_dedup.to_csv('DLQ_120.csv',sep=',')
Prepay_dedup.to_csv('Prepay.csv',sep=',')
all_loan_dedup.to_csv('all_loan.csv',sep=',')



################################################################
# Transition Matrix for payment activity 

#test
testdata=pd.read_csv("FalconMortgageSample.csv",delimiter="|")

seq=testdata.groupby('NUM_LOAN')['DLQPeriod'].apply(lambda x: "{%s}" % ', '.join(x))

seq=testdata.groupby('NUM_LOAN')['DLQPeriod'].apply(lambda x: x.sum())


data=seq
seq['DT2_BUSINESS'] =  pd.to_datetime(seq['DT2_BUSINESS'], format='%m/%d/%Y')
data_sorted = seq.sort_values(['NUM_LOAN','DT2_BUSINESS'], ascending=True)

data.groupby('DLQPeriod').NUM_LOAN.count().sort_values()



seq['DLQPeriod']=seq.groupby(['NUM_LOAN'])['DLQPeriod'].bfill()
seq['DLQPeriod']=seq.groupby(['NUM_LOAN'])['DLQPeriod'].ffill()

data.groupby('DLQPeriod').NUM_LOAN.count().sort_values()
seq.groupby('NUM_LOAN').DLQPeriod.nunique().sort_values()


06/2017

test = data.loc[(data["LoanFundedDate"] >='2017-06-01 00:00:00' ),["NUM_LOAN","DT2_BUSINESS","LoanFundedDate","LOAN_PORTFOLIO"]]
test2 = test.loc[(test["LoanFundedDate"] < '2017-07-01 00:00:00' ),["NUM_LOAN","DT2_BUSINESS","LoanFundedDate","LOAN_PORTFOLIO"]]

check1=data.groupby('DT2_BUSINESS').LOAN_PORTFOLIO.nunique().sort_values()

#################################################

# data checking 
# count by group
df.groupby(['outlier_FICO']).size()

# count number of unique values 
data.groupby('NUM_LOAN').PropertyType.nunique()

# count of missing value
data['DLQPeriod'].isnull().sum()

# check origination information consistent by loans 
check1=data.groupby('NUM_LOAN').DLQPeriod.nunique().sort_values()
check1[check1>1]

# not good
check2=data.groupby('NUM_LOAN').PropertyType.nunique().sort_values()
check2[check2>1]

data.loc[(data["NUM_LOAN"]==6000507475), ["DT2_BUSINESS","NUM_LOAN","PropertyType","LoanType","OrigASSETS","OrigAPR"]].sort_values(["DT2_BUSINESS"], ascending=True)



check3=data.groupby('NUM_LOAN').LoanMaturityDate.nunique().sort_values()
test=check3[check3>1]

data.loc[(data["NUM_LOAN"]==6007548933), ["DT2_BUSINESS","NUM_LOAN","LoanMaturityDate","LoanType","OrigASSETS","OrigAPR"]].sort_values(["DT2_BUSINESS"], ascending=True)



data['tenure'] = (data.DT2_BUSINESS - data.LoanFundedDate)/ np.timedelta64(1, 'M')
data[["DT2_BUSINESS","LoanFundedDate","tenure"]].sort_values(["DT2_BUSINESS"], ascending=True)


data[['11-01-2017','LoanPaidInFullDate','LoanCancellationDate']].min(axis=1)

datetime.now().date()

where(df[['age', 'url']].isnull(), 1)

min(data["LoanPaidInFullDate"],data["LoanCancellationDate"])

data[["11-01-2017",'LoanPaidInFullDate','LoanCancellationDate']].min(axis=1)

data.groupby('DLQPeriod').NUM_LOAN.count().sort_values()
seq.groupby('NUM_LOAN').DLQPeriod.nunique().sort_values()






# profiling on the mortgage vintage file 

mtg_v=pd.read_csv("MTG_VINTAGE_20171031_VER_1.1.csv",delimiter="|")
# 345,498 distinct loans 

# change type to date
mtg_v['DT2_BUSINESS'] =  pd.to_datetime(mtg_v['DT2_BUSINESS'], format='%m/%d/%Y')
mtg_v['LoanFundedDate'] =  pd.to_datetime(mtg_v['LoanFundedDate'], format='%m/%d/%Y')

mtg2_v=mtg_v.loc[(mtg_v["LoanFundedDate"] >='2012-01-01 00:00:00' )]


# check duplication
check1=mtg2_v.groupby('IDN_MS_LOAN').OrigMedianCreditScore.nunique().sort_values()
check1[check1>1]


mtg2_v.loc[(mtg2_v["IDN_MS_LOAN"] ==1539365 ),["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","LOAN_PORTFOLIO","OrigMedianCreditScore"]].sort_values(["DT2_BUSINESS"], ascending=True)


# keep last records
mtg2_dedup=mtg2_v.sort('DT2_BUSINESS').drop_duplicates(subset='IDN_MS_LOAN', keep="last")


mtg2_dedup.to_csv('Mtg_vintage_dedup.csv')


# check consistency 

check2=test.groupby('IDN_MS_LOAN').LOAN_PORTFOLIO.nunique().sort_values()
check2[check2>1]

LOAN_PORTFOLIO
LoanType
LoanPurpose
OrigMedianCreditScore

#Data Quality check 
################################################################
# Amex file checking 
pcm_amex=pd.read_csv("PartytoPCMAmex1.1.csv",delimiter="|")

temp=pcm_amex.groupby(['uuid', 'dt2_business']).size()

temp= pcm_amex.groupby('uuid').dt2_business.nunique()
temp[temp>1]

temp2=temp[temp>1]



pcm_amex.uuid.nunique()
#38784435 uuids


pcm_amex.loc[(pcm_amex["uuid"]in ("00003e23-d400-475d-9e21-dcb05bfd0c8f","ffffe9a0-2030-48e4-a560-7db1e6a21605")),["uuid","dt2_business","amexindicator"]].sort_values(["dt2_business"], ascending=True)
pcm_amex.loc[(pcm_amex["uuid"]=="ffffe9a0-2030-48e4-a560-7db1e6a21605"),["uuid","dt2_business","amexindicator"]].sort_values(["dt2_business"], ascending=True)


################################################################
# FalconMortgageLoantoPartyId2.0 (updated on 11/27/17)
loantoparty=pd.read_csv("MTG_VINTAGE_PARTY_V1.3.csv",delimiter="|")

# check merge rate with internal mortgage file
mtg_new=mtg_v[["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","Fund_Year"]]
mtg_party=pd.merge(mtg_new, loantoparty, left_on='IDN_MS_LOAN', right_on='idn_ms_loan', how='outer')

mtg_party['MSLoanNumber'].isnull().sum()

# find null matched records 
loan_null=mtg_party[mtg_party['IDN_MS_LOAN'].isnull()]
party_null=mtg_party[mtg_party['idn_ms_loan'].isnull()]

# count of loans that doesn't match
loan_null.idn_ms_loan.nunique()
party_null.IDN_MS_LOAN.nunique()

# vintage mortgage null match records distribution by years 
party_null.groupby(['Fund_Year'])['LastPaymentDate'].apply(lambda x: x.isnull().sum())
party_null.groupby('Fund_Year').IDN_MS_LOAN.nunique()


# output examples 
loan_null.to_csv('loan_null.csv')

test=party_null[party_null['Fund_Year']>2011]
test1=test.IDN_MS_LOAN.unique()
output=mtg_v[mtg_v['IDN_MS_LOAN'].isin(test1)]



################################################################
# FalconMortgageLoantoPartyId2.0 (old)
loantoparty=pd.read_csv("FalconMortgageLoantoPartyId2.0.csv",delimiter="|")

# check merge rate with internal mortgage file
mtg_new=mtg_v[["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","Fund_Year"]]
mtg_party=pd.merge(mtg_new, loantoparty, left_on='IDN_MS_LOAN', right_on='MSLoanNumber', how='outer')

mtg_party['MSLoanNumber'].isnull().sum()

# find null matched records 
loan_null=mtg_party[mtg_party['IDN_MS_LOAN'].isnull()]
party_null=mtg_party[mtg_party['MSLoanNumber'].isnull()]

# count of loans that doesn't match
loan_null.MSLoanNumber.nunique()
party_null.IDN_MS_LOAN.nunique()

# vintage mortgage null match records distribution by years 
party_null.groupby(['Fund_Year'])['LastPaymentDate'].apply(lambda x: x.isnull().sum())
party_null.groupby('Fund_Year').IDN_MS_LOAN.nunique()


# output examples 
loan_null.to_csv('loan_null.csv')

test=party_null[party_null['Fund_Year']>2011]
test1=test.IDN_MS_LOAN.unique()
output=mtg_v[mtg_v['IDN_MS_LOAN'].isin(test1)]


################################################################
# Mortgage_Missing_Party.csv
mtg_miss=pd.read_csv("Mortgage_Missing_Party.csv",delimiter="|")

# change to date type
mtg_miss['DT2_BUSINESS'] =  pd.to_datetime(mtg_miss['DT2_BUSINESS'], format='%m/%d/%Y')
mtg_miss['LoanFundedDate'] =  pd.to_datetime(mtg_miss['LoanFundedDate'], format='%m/%d/%Y')

# create funded year col 
mtg_miss['Fund_Year']=mtg_miss.LoanFundedDate.dt.year

mtg_miss.groupby('Fund_Year').IDN_MS_LOAN.nunique()


# exclude missing party population from all mortgage data

mtg_exclude=mtg_v[~mtg_v['IDN_MS_LOAN'].isin(mtg_miss['IDN_MS_LOAN'])]


# Fund year
mtg_exclude.groupby('Fund_Year').IDN_MS_LOAN.nunique()
mtg_miss.groupby('Fund_Year').IDN_MS_LOAN.nunique()

# Loan Type 
mtg_exclude.groupby('LoanType').IDN_MS_LOAN.nunique()
mtg_miss.groupby('LoanType').IDN_MS_LOAN.nunique()

# Disposition Type
mtg_miss.groupby('NME_DISPOSITION_TYPE').IDN_MS_LOAN.nunique()





################################################################
# PLA_Missing_Party.csv
pla_miss=pd.read_csv("PLA_Missing_Party.csv",delimiter="|")

pla_miss.IDN_MSTR_FACIL.nunique()
# 4857
pla_miss.IDN_MSTR_FACIL.count()
# 4857

pla_miss.groupby('CDE_PAID_STATUS').IDN_MSTR_FACIL.nunique()

# create funded year
pla_miss['DT2_ISSUE'] =  pd.to_datetime(pla_miss['DT2_ISSUE'], format='%m/%d/%Y')
pla_miss['Fund_Year']=pla_miss.DT2_ISSUE.dt.year

pla_miss.groupby('Fund_Year').IDN_MSTR_FACIL.nunique()

# check overlap
pla.loc[pla['IDN_MSTR_FACIL']==7540310375]

# exclude missing party population from all mortgage data
pla_exclude=pla[~pla['IDN_MSTR_FACIL'].isin(pla_miss['IDN_MSTR_FACIL'])]


pla.loc[pla['IDN_MSTR_FACIL']==7540310375]

lal_pla=lal_pla[lal_pla['dt2_business']=='2017-10-31']




################################################################
# NON_CONVERTED_PLA_PARTY_V1.2.csv
platoparty=pd.read_csv("NON_CONVERTED_PLA_PARTY_V1.2.csv",delimiter="|")

# check merge rate with PLA
pla_party=pd.merge(pla, platoparty, left_on='IDN_MSTR_FACIL', right_on='LOAN_IDENTIFIER', how='outer')

pla_party['IDN_MSTR_FACIL'].isnull().sum()
# 0
pla_party[pla_party['LOAN_IDENTIFIER'].isnull()].IDN_MSTR_FACIL.nunique()
# 4931 loans

# find null matched records 
loan_null=pla_party[pla_party['IDN_MSTR_FACIL'].isnull()]
party_null=pla_party[pla_party['LOAN_IDENTIFIER'].isnull()]

# count of loans that doesn't match
loan_null.LOAN_IDENTIFIER.nunique()
party_null.IDN_MSTR_FACIL.nunique()




# output examples 
loan_null.to_csv('loan_null.csv')

test=party_null[party_null['Fund_Year']>2011]
test1=test.IDN_MS_LOAN.unique()
output=mtg_v[mtg_v['IDN_MS_LOAN'].isin(test1)]



################################################################
# Non Converted PLA 
# NON_CONVERTED_PLA_V1.0.csv
pla=pd.read_csv("NON_CONVERTED_PLA_V1.0.csv",delimiter="|")

pla['DT2_BUSINESS'] =  pd.to_datetime(pla['DT2_BUSINESS'], format='%m/%d/%Y')


# check if there are two record for each month 
dups=pla.groupby(['IDN_MSTR_FACIL','DT2_BUSINESS']).size().loc[lambda x: x>1].sort_values()
# 0 loans


# check if value is negative 
test=pla[pla['TOTAL_MTD_BORROWED']<0]
test.IDN_MSTR_FACIL.nunique()
'''
OUTSTANDING_BALANCE
# 0
FACILITY_AMOUNT
# 0 loans 
AMT_COLLATERAL_ADVANCE
# 5,529 loans
COLLATERAL_MKT_VALUE
# 159 loans 
AMT_ACR_MTD_INT
# 887 loans
TOTAL_MTD_PAID
# 4 loans 
TOTAL_MTD_BORROWED
# 1 loans
'''

# check monthly total count and average value
pla_aggre=pla.groupby('DT2_BUSINESS').agg({'OUTSTANDING_BALANCE':['size','mean'],'FACILITY_AMOUNT':['size','mean'],'AMT_COLLATERAL_ADVANCE':['size','mean'],'COLLATERAL_MKT_VALUE':['size','mean']})
pla_aggre.to_csv('aggre_pla.csv')



################################################################
# FalconLAL_PLAConverted1.2.csv (updated 11/21/17)
lal_pla=pd.read_csv("FalconLAL_PLAConverted1.2.csv",delimiter="|")

lal_pla['dt2_business'] =  pd.to_datetime(lal_pla['dt2_business'], format='%m/%d/%Y')


# check if there are two record for each month 
dups=lal_pla.groupby(['keyuuid','dt2_business']).size().loc[lambda x: x>1].sort_values()
# 0 loans


# check if value is negative 
test=lal_pla[lal_pla['facility_amount']<0]
test.keyuuid.nunique()

'''
facility_amount
# 194 loans 
amt_collateral_advance
# 13,449 loans
collateral_mkt_value
# 247 loans 
amt_int_mtd
# 1868 loans
total_mtd_paid
# 52,768 loans 
total_mtd_borrowed
# 3 loans
'''

test=lal_pla[lal_pla['cde_facil_type'].isnull()]
lal_pla.loc[(lal_pla["keyuuid"]=='897b761c-435c-4ad9-9cbe-e093333b4f3d')].sort_values(["dt2_business"], ascending=True)

test=lal_pla[lal_pla['amt_int_mtd'].isnull()]
test1=lal_pla.loc[(lal_pla["keyuuid"]=='86e0efcb-a555-42a1-89d7-71af16f6dc0d'),["keyuuid","dt2_business","dt2_facil_acct_book","amt_int_mtd"]].sort_values(["dt2_business"], ascending=True)

test=lal_pla[lal_pla['total_mtd_paid'].isnull()]
test1=lal_pla.loc[(lal_pla["keyuuid"]=='8a3a3c7f-590c-4289-92aa-4ef94e23361f'),["keyuuid","dt2_business","dt2_facil_acct_book","cde_facil_status","total_mtd_paid"]].sort_values(["dt2_business"], ascending=True)

test=lal_pla[lal_pla['total_mtd_borrowed'].isnull()]
test1=lal_pla.loc[(lal_pla["keyuuid"]=='5da8f8bf-c011-4f62-bdab-45f73eabede2'),["keyuuid","dt2_business","dt2_facil_acct_book","cde_facil_status","total_mtd_borrowed","idn_pla_mstr_facil"]].sort_values(["dt2_business"], ascending=True)


# check monthly total count and average value
lal_pla_aggre=lal_pla.groupby('dt2_business').agg({'outstanding_balance':['size','mean'],'facility_amount':['size','mean'],'amt_collateral_advance':['size','mean'],'collateral_mkt_value':['size','mean']})
lal_pla_aggre.to_csv('aggre_lal_pla.csv')



################################################################
# FalconLALPLALoans.csv (old)
lal_pla=pd.read_csv("FalconLALPLALoans.csv",delimiter="|")

lal_pla['dt2_business'] =  pd.to_datetime(lal_pla['dt2_business'], format='%m/%d/%Y')

# check if value is negative 
test=lal_pla[lal_pla['total_mtd_borrowed']<0]
test.keyuuid.nunique()

'''
facility_amount
amt_collateral_advance
collateral_mkt_value
amt_int_mtd
total_mtd_paid
total_mtd_borrowed'''

test=lal_pla[lal_pla['cde_facil_type'].isnull()]
lal_pla.loc[(lal_pla["keyuuid"]=='897b761c-435c-4ad9-9cbe-e093333b4f3d')].sort_values(["dt2_business"], ascending=True)

test=lal_pla[lal_pla['amt_int_mtd'].isnull()]
test1=lal_pla.loc[(lal_pla["keyuuid"]=='86e0efcb-a555-42a1-89d7-71af16f6dc0d'),["keyuuid","dt2_business","dt2_facil_acct_book","amt_int_mtd"]].sort_values(["dt2_business"], ascending=True)

test=lal_pla[lal_pla['total_mtd_paid'].isnull()]
test1=lal_pla.loc[(lal_pla["keyuuid"]=='8a3a3c7f-590c-4289-92aa-4ef94e23361f'),["keyuuid","dt2_business","dt2_facil_acct_book","cde_facil_status","total_mtd_paid"]].sort_values(["dt2_business"], ascending=True)

test=lal_pla[lal_pla['total_mtd_borrowed'].isnull()]
test1=lal_pla.loc[(lal_pla["keyuuid"]=='5da8f8bf-c011-4f62-bdab-45f73eabede2'),["keyuuid","dt2_business","dt2_facil_acct_book","cde_facil_status","total_mtd_borrowed","idn_pla_mstr_facil"]].sort_values(["dt2_business"], ascending=True)


# check monthly total count and average value
lal_pla_aggre=lal_pla.groupby('dt2_business').agg({'outstanding_balance':['size','mean'],'facility_amount':['size','mean'],'amt_collateral_advance':['size','mean'],'collateral_mkt_value':['size','mean']})
lal_pla_aggre.to_csv('aggre_lal_pla.csv')



################################################################
# LAL_PLA_CONVERTED_MARGIN_V1.2.csv (updated 11/27/17)
lal_pla_mcall=pd.read_csv("LAL_PLA_CONVERTED_MARGIN_V1.2.csv",delimiter="|")

lal_pla_mcall['MONTH_END_DATE'] =  pd.to_datetime(lal_pla_mcall['MONTH_END_DATE'], format='%m/%d/%Y')


# check monthly total count and average value
lal_mcall_aggre=lal_pla_mcall.groupby('MONTH_END_DATE').agg({'AMT_CALL':['size','mean'],'OUTSTANDING_BALANCE':['size','mean']})
lal_mcall_aggre.to_csv('aggre_lal_mcall.csv')


# check if amt_call > 0
test=lal_pla_mcall[lal_pla_mcall['OUTSTANDING_BALANCE']<0]
test.FACILITY_KEY_ACCOUNT.nunique()
# 0 loans

dups=lal_pla_mcall.groupby(['FACILITY_KEY_ACCOUNT','MONTH_END_DATE']).size().loc[lambda x: x>1].sort_values()

lal_pla_mcall.loc[lal_pla_mcall["FACILITY_KEY_ACCOUNT"]=='ccd52604-8207-40c9-82c8-bbd6d984b278'].sort_values(["MONTH_END_DATE"], ascending=True)


################################################################
# NON_CONVERTED_PLA_MARGIN_V1.0.csv (updated 11/27/17)
pla_mcall=pd.read_csv("NON_CONVERTED_PLA_MARGIN_V1.0.csv",delimiter="|")

pla_mcall['MONTH_END_DATE'] =  pd.to_datetime(pla_mcall['MONTH_END_DATE'], format='%m/%d/%Y')


# check monthly total count and average value
pla_mcall_aggre=pla_mcall.groupby('MONTH_END_DATE').agg({'AMT_CALL':['size','mean'],'OUTSTANDING_BALANCE':['size','mean']})
pla_mcall_aggre.to_csv('aggre_pla_mcall.csv')


# check if amt_call > 0
test=pla_mcall[pla_mcall['AMT_CALL']<0]
test.IDN_MSTR_FACIL.nunique()
# 0 loans

test=pla_mcall[pla_mcall['OUTSTANDING_BALANCE']<0]
test.IDN_MSTR_FACIL.nunique()
# 0 loans


################################################################
# FalconLALPLAMargincall.csv
lal_pla_mcall=pd.read_csv("FalconLALPLAMargincall.csv",delimiter="|")

lal_pla_mcall['calendardate'] =  pd.to_datetime(lal_pla_mcall['calendardate'], format='%m/%d/%Y')


# check monthly total count and average value
lal_mcall_aggre=lal_pla_mcall.groupby('calendardate').agg({'amt_call':['size','mean'],'outstanding_balance':['size','mean'],'amt_collateral_advance':['size','mean']})
lal_mcall_aggre.to_csv('aggre_lal_mcall.csv')

# check if there are two record for each month 
dups=lal_pla_mcall.groupby(['keyuuid','calendardate']).size().loc[lambda x: x>1].sort_values()

lal_pla_mcall.loc[lal_pla_mcall["keyuuid"]=='0a22254f-b458-4c0d-b3e8-d57b52f8d21b'].sort_values(["calendardate"], ascending=True)

# check if amt_call > 0
test=lal_pla_mcall[lal_pla_mcall['amt_call']>0]
test.keyuuid.nunique()



################################################################
# FalconRawAssetPartyLevel.csv
asset=pd.read_csv("FalconRawAssetPartyLevel.csv",delimiter="|")

# create month end date 
from pandas.tseries.offsets import MonthEnd
asset['Day'] = 1
asset['yearm']=pd.to_datetime(asset[['Year', 'Month', 'Day']])
asset['calendardate'] = pd.to_datetime(asset['yearm'], format="%Y%m") + MonthEnd(1)

asset=asset.drop(['Day','yearm'],1)

# sum asset by party and month 
aseet_zero=asset.groupby(['uuid','calendardate'])['AssetAmt'].sum()
asset_good=aseet_zero[(aseet_zero["AssetAmt"] >1) | (aseet_zero["AssetAmt"]<-1)]
len(asset_good)
# 121,322,025
len(aseet_zero)
# 123314405

aseet_all=asset.groupby(['calendardate'])['uuid'].nunique()
#123,314,405

asset.uuid.nunique()
# 5,391,436


################################################################
# FalconCashActivity1.0.csv
cash=pd.read_csv("FalconCashActivity1.0.csv",delimiter="|")

# change date format
cash['calendardate'] =  pd.to_datetime(cash['calendardate'], format='%m/%d/%Y')

cash.loc[cash["keyuuid"]=='8dce463d-20f2-44da-8cb1-20f8c8358bd7'].sort_values(["calendardate"], ascending=True)

# check rows with 0
check=cash[(cash["amt_inflow"] ==0) & (cash["cnt_inflow"]==0) & (cash["amt_outflow"]==0) & (cash["cnt_outflow"]==0)]
check['sum']=check.amt_inflow + check.cnt_inflow + check.amt_outflow + check.cnt_outflow



# select example where value is negative
test=cash[cash['cnt_outflow']<0]
test.keyuuid.unique()
test1=cash.loc[cash["keyuuid"]=='99b0dd4e-69f9-489b-8c9a-e5f03f319187'].sort_values(["calendardate"], ascending=True)

# check if there are two record for each month 
dups=cash.groupby(['keyuuid','calendardate']).size().loc[lambda x: x>1].sort_values()

# check monthly total count and average value
cash_aggre=cash.groupby('calendardate').agg({'amt_inflow':['size','sum'],'cnt_inflow':['size','sum'],'amt_outflow':['size','sum'],'cnt_outflow':['size','sum']})
cash_aggre.to_csv('cash_aggre.csv')







################################################################
# FalconSecActivity1.0.csv
sec=pd.read_csv("FalconSecActivity1.0.csv",delimiter="|")

# change date format
sec['calendardate'] =  pd.to_datetime(sec['calendardate'], format='%m/%d/%Y')

dups=sec.groupby(['keyuuid','calendardate']).size().loc[lambda x: x>1].sort_values() 

sec.loc[1]

# check zero value for all the fields: 41%
check=sec[(sec["buy_total_trade_amt"] ==0) & (sec["sell_total_trade_amt"]==0) & (sec["total_buy_trade_count"]==0) & (sec["total_sell_trade_count"]==0)]
len(check)
len(sec)
# 137772679
# 337579806
check2=check.groupby(['calendardate']).size() 
check2.to_csv("check.csv") 


# select example where value is negative
test=sec[sec['total_sell_trade_count']<0]
test.keyuuid.unique()
test1=sec.loc[sec["keyuuid"]=='e59d0f7d-2169-4a79-a5b2-a35a7e789776'].sort_values(["calendardate"], ascending=True)


'''
buy_total_trade_amt
sell_total_trade_amt
total_buy_trade_count
total_sell_trade_count'''

# check jump from 2010/09/30 to 2010/10/31

sep2010=sec[sec['calendardate']=='2010/09/30']
aug2010=sec[sec['calendardate']=='2010/08/31']

# aug2010.keyuuid.nunique()
1040127
# sep2010.keyuuid.nunique()
3217635




################################################################
# margin balance
margin=pd.read_csv("FalconMarginBalance1.3.csv",delimiter="|")

# create month end date 
from pandas.tseries.offsets import MonthEnd
margin['Day'] = 1
margin['yearm']=pd.to_datetime(margin[['Year', 'Month', 'Day']])
margin['calendardate'] = pd.to_datetime(margin['yearm'], format="%Y%m") + MonthEnd(1)

margin=margin.drop(['Day','yearm'],1)

# check if margin balance are positive
test = margin[margin['MarginBalance']>0]

# check if there are two record for each month 
dups=margin.groupby(['keyuuid','calendardate']).size().loc[lambda x: x>1].sort_values()

# check monthly total count and average value
margin_aggre=margin.groupby('calendardate').agg({'MarginBalance':['size','sum']})
margin_aggre.to_csv('margin_aggre.csv')



################################################################
# employee flag 
emply=pd.read_csv("FalconIsEmpIsDeceased.csv",delimiter="|")

emply[emply['isDeceased'].isnull()]

emply.groupby(['uuid']).size()

dups=emply.groupby(['uuid']).size().loc[lambda x: x>1].sort_values()

emply[emply['uuid']=='45676304-5e98-4f84-907c-d0cfb9c0eb22']







################################################################
# Mortgage vintage file profiling (updated 11/21/17)
mtg_v=pd.read_csv("MTG_VINTAGE_20171031_VER_1.1.csv",delimiter="|")
# 345,498 distinct loans 

# change type to date
mtg_v['DT2_BUSINESS'] =  pd.to_datetime(mtg_v['DT2_BUSINESS'], format='%m/%d/%Y')
mtg_v['LoanFundedDate'] =  pd.to_datetime(mtg_v['LoanFundedDate'], format='%m/%d/%Y')


# check loan origination by year
mtg_v['Fund_Year']=mtg_v.LoanFundedDate.dt.year

# total count of loans 
afterfund["IDN_MS_LOAN"].nunique()
# 327,747

# check if there are two record for each month 
dups=mtg_v.groupby(['IDN_MS_LOAN','DT2_BUSINESS']).size().loc[lambda x: x>1].sort_values()
# 4 loans
tes1=mtg_v.loc[(mtg_v["IDN_MS_LOAN"]==1235709)].sort_values(["DT2_BUSINESS"], ascending=True)




# check inconsistency 
# LoanFundedDate
check1=afterfund.groupby('IDN_MS_LOAN').LoanFundedDate.nunique().sort_values()
check2=check1[check1>1]
# 165 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1550692), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate"]].sort_values(["DT2_BUSINESS"], ascending=True)

# LoanCancellationDate
check1=afterfund.groupby('IDN_MS_LOAN').LoanCancellationDate.nunique().sort_values()
check2=check1[check1>1]
# 2 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1588475), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","LoanCancellationDate"]].sort_values(["DT2_BUSINESS"], ascending=True)

# BaseLoanAmount
check1=afterfund.groupby('IDN_MS_LOAN').BaseLoanAmount.nunique().sort_values()
check2=check1[check1>1]
# 607 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1215834), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","BaseLoanAmount"]].sort_values(["DT2_BUSINESS"], ascending=True)

# NME_DISPOSITION_TYPE
check1=afterfund.groupby('IDN_MS_LOAN').NME_DISPOSITION_TYPE.nunique().sort_values()
check2=check1[check1>1]
# 61,285 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1207407), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","NME_DISPOSITION_TYPE"]].sort_values(["DT2_BUSINESS"], ascending=True)

# NME_PYMT_IN_FULL_STOP
check1=afterfund.groupby('IDN_MS_LOAN').NME_PYMT_IN_FULL_STOP.nunique().sort_values()
check2=check1[check1>1]
# 11,052 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1232036), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","NME_PYMT_IN_FULL_STOP"]].sort_values(["DT2_BUSINESS"], ascending=True)






################################################################
# Mortgage vintage file profiling (old)
mtg_v=pd.read_csv("MTG_VINTAGE.csv",delimiter="|")
# 345,498
# change type to date
mtg_v['DT2_BUSINESS'] =  pd.to_datetime(mtg_v['DT2_BUSINESS'], format='%m/%d/%Y')
mtg_v['LoanFundedDate'] =  pd.to_datetime(mtg_v['LoanFundedDate'], format='%m/%d/%Y')

# check loan origination by year
mtg_v['Fund_Year']=mtg_v.LoanFundedDate.dt.year
mtg_v.groupby('Fund_Year').IDN_MS_LOAN.nunique().sort_values()

# select all records after loan origination date
afterfund = mtg_v[mtg_v['DT2_BUSINESS']>=mtg_v['LoanFundedDate']]

mtg_v.groupby('Fund_Year').IDN_MS_LOAN.nunique().sort_values()

# total count of loans 
afterfund["IDN_MS_LOAN"].nunique()
# 327,747

# count number of missing values 
afterfund['OrigMedianCreditScore'].isnull().sum()
afterfund['ApplicationDate'].isnull().sum()
afterfund['PropertyType'].isnull().sum()
afterfund['OccupancyType'].isnull().sum()
afterfund['IND_FirstTimeHomeBuyer'].isnull().sum()

#Missing rate and example 
# check the missing percentage for PAndIPayment after loan funded 
afterfund = mtg_v[mtg_v['DT2_BUSINESS']<mtg_v['LoanFundedDate']]

afterfund[["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PAndIPayment"]].sort_values(["IDN_MS_LOAN","DT2_BUSINESS"], ascending=True)

afterfund.loc[(afterfund["IDN_MS_LOAN"]==1511429), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PAndIPayment"]].sort_values(["DT2_BUSINESS"], ascending=True)

len(mtg_v)
# 44,036,890

len(afterfund)
# 43,161,261

afterfund['PAndIPayment'].isnull().sum()
# 27,453,177


# OrigMedianCreditScore
afterfund.groupby(['Fund_Year'])['OrigMedianCreditScore'].apply(lambda x: x.isnull().sum())

afterfund[[afterfund['OrigMedianCreditScore'] is not None & afterfund['Fund_Year']=='2010.0']]

test=afterfund[afterfund['OrigMedianCreditScore'].isnull()]
test.loc[test['Fund_Year']==2010,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OrigMedianCreditScore"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1241485), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OrigMedianCreditScore"]].sort_values(["DT2_BUSINESS"], ascending=True)

# ApplicationDate
afterfund.groupby(['Fund_Year'])['ApplicationDate'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['ApplicationDate'].isnull()]
test.loc[test['Fund_Year']==2014,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","ApplicationDate"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1569364), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","ApplicationDate"]].sort_values(["DT2_BUSINESS"], ascending=True)

# PropertyType
afterfund.groupby(['Fund_Year'])['PropertyType'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['PropertyType'].isnull()]
test.loc[test['Fund_Year']==2002,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PropertyType"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1100371), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PropertyType"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')

# OccupancyType
afterfund.groupby(['Fund_Year'])['OccupancyType'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['OccupancyType'].isnull()]
test.loc[test['Fund_Year']==2005,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OccupancyType"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1314644), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OccupancyType"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')

# OccupancyType
afterfund.groupby(['Fund_Year'])['OccupancyType'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['OccupancyType'].isnull()]
test.loc[test['Fund_Year']==2002,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OccupancyType"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1100371), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OccupancyType"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')

# IND_FirstTimeHomeBuyer
afterfund.groupby(['Fund_Year'])['IND_FirstTimeHomeBuyer'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['IND_FirstTimeHomeBuyer'].isnull()]
test.loc[test['Fund_Year']==2005,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","IND_FirstTimeHomeBuyer"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1314644), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","IND_FirstTimeHomeBuyer"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')

# LastPaymentDate
afterfund.groupby(['Fund_Year'])['LastPaymentDate'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['LastPaymentDate'].isnull()]
test.loc[test['Fund_Year']==2012,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","LastPaymentDate"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1536881), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","LastPaymentDate"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')

# PRODUCT
afterfund.groupby(['Fund_Year'])['PRODUCT'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['PRODUCT1'].isnull()]
test1=test.loc[test['Fund_Year']==2011,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PRODUCT"]].sort_values(["IDN_MS_LOAN"], ascending=True)

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1247517), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PRODUCT"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')

afterfund['PRODUCT1']=afterfund['PRODUCT']
afterfund['PRODUCT1']=afterfund.groupby(['IDN_MS_LOAN'])['PRODUCT1'].ffill()
afterfund['PRODUCT1']=afterfund.groupby(['IDN_MS_LOAN'])['PRODUCT1'].bfill()

afterfund.groupby(['Fund_Year'])['PRODUCT1'].apply(lambda x: x.isnull().sum())

# PAndIPayment
afterfund.groupby(['Fund_Year'])['PAndIPayment'].apply(lambda x: x.isnull().sum())

test=afterfund[afterfund['PAndIPayment'].isnull()]
test.loc[test['Fund_Year']==2012,["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PAndIPayment"]]

test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1536881), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","PAndIPayment"]].sort_values(["DT2_BUSINESS"], ascending=True)
test1.to_csv('test1.csv')




# check pledgetype Non value 
data.groupby('PledgeType').count()



# check origination information for account 
check1=afterfund.groupby('IDN_MS_LOAN').OrigLTV.nunique().sort_values()
check1[check1>1]
# 98 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1595901), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OrigLTV","OrigCombinedLTV"]].sort_values(["DT2_BUSINESS"], ascending=True)


check2=afterfund.groupby('IDN_MS_LOAN').OrigCombinedLTV.nunique().sort_values()
check2[check2>1]
# 262 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1584073), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OrigLTV","OrigCombinedLTV"]].sort_values(["DT2_BUSINESS"], ascending=True)

check3=afterfund.groupby('IDN_MS_LOAN').OrigDTI.nunique().sort_values()
check3[check3>1]
# 26,376 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1571385), ["IDN_MS_LOAN","DT2_BUSINESS","ApplicationDate","LoanFundedDate","OrigDTI"]].sort_values(["DT2_BUSINESS"], ascending=True)

check4=afterfund.groupby('IDN_MS_LOAN').OrigASSETS.nunique().sort_values()
check4[check4>1]
# 37,602 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1214620), ["IDN_MS_LOAN","DT2_BUSINESS","ApplicationDate","LoanFundedDate","OrigASSETS"]].sort_values(["DT2_BUSINESS"], ascending=True)

check5=afterfund.groupby('IDN_MS_LOAN').OrigMedianCreditScore.nunique().sort_values()
check5[check5>1]
# 142 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1573769), ["IDN_MS_LOAN","DT2_BUSINESS","ApplicationDate","LoanFundedDate","OrigMedianCreditScore"]].sort_values(["DT2_BUSINESS"], ascending=True)

check6=afterfund.groupby('IDN_MS_LOAN').OrigRate.nunique().sort_values()
check6[check6>1]
# 21,271 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1219486), ["IDN_MS_LOAN","DT2_BUSINESS","ApplicationDate","LoanFundedDate","OrigRate"]].sort_values(["DT2_BUSINESS"], ascending=True)

check7=afterfund.groupby('IDN_MS_LOAN').OrigNoteRate.nunique().sort_values()
check7[check7>1]
# 4 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1607435), ["IDN_MS_LOAN","DT2_BUSINESS","ApplicationDate","LoanFundedDate","OrigNoteRate"]].sort_values(["DT2_BUSINESS"], ascending=True)

check8=afterfund.groupby('IDN_MS_LOAN').LoanFundedDate.nunique().sort_values()
check8[check8>1]
# 128 loans 
afterfund.loc[(afterfund["IDN_MS_LOAN"]==1546613), ["IDN_MS_LOAN","DT2_BUSINESS","ApplicationDate","LoanFundedDate"]].sort_values(["DT2_BUSINESS"], ascending=True)


check1=afterfund.groupby('IDN_MS_LOAN').BaseLoanAmount.nunique().sort_values()
check1[check1>1]
# 607 loans 
test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1220680), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","BaseLoanAmount"]].sort_values(["DT2_BUSINESS"], ascending=True)


check2=afterfund.groupby('IDN_MS_LOAN').LoanTerm.nunique().sort_values()
check2[check2>1]
# 3,454 loans 
test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1163169), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","LoanTerm"]].sort_values(["DT2_BUSINESS"], ascending=True)

check3=afterfund.groupby('IDN_MS_LOAN').OccupancyType.nunique().sort_values()
check3[check3>1]
# 311 loans 
test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1228014), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OccupancyType"]].sort_values(["DT2_BUSINESS"], ascending=True)

check4=afterfund.groupby('IDN_MS_LOAN').OrigAppraisalValue.nunique().sort_values()
check4[check4>1]
# 132 loans
test1=afterfund.loc[(afterfund["IDN_MS_LOAN"]==1548451), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OrigAppraisalValue"]].sort_values(["DT2_BUSINESS"], ascending=True)




afterfund.loc[(afterfund["IDN_MS_LOAN"]==1034219), ["IDN_MS_LOAN","DT2_BUSINESS","LoanFundedDate","OrigRate"]].sort_values(["DT2_BUSINESS"], ascending=True)


test=afterfund[afterfund['OrigRate']>15]
test1=test[test['Fund_Year']>2000]

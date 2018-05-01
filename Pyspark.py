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

# output data into local 
def exporttocsv(query,filename):
    
    result = sqlContext.sql(query)
    
    
    names=[item[0] for item in result.dtypes]
    df=pd.DataFrame(result.collect(),columns=names)
    
    df.to_csv(filename,index=False)
    
exporttocsv("select * from dl_proj4_work.t_jtu_master_asset distribute by rand() sort by rand() limit 100000","/v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/Falcon_Nonpii/PreAcxiom/InputTables/EDD/data_master_asset3.csv")




# edd pyspark version 
def edd(query):
    df = sqlContext.sql(query)
    df.cache()
    
    des  = df.describe()
    dist = df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))
    
    new = sqlContext.createDataFrame([("dist_count",)],['summary'])
    new3= new.crossJoin(dist)
    edd = des.union(new3)
    
    edd.toPandas().to_csv("/v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/Falcon_Nonpii/PreAcxiom/InputTables/EDD/EDD_Master.csv")
    
    df.unpersist()

edd("select * from dl_proj4_work.t_jtu_master_combined")






# scala code to combine dataset

import org.apache.spark.sql.hive.HiveContext

def master_combine() : Unit = {
    
    val sqlContext = new HiveContext(sc)

    val common_attributes = Seq("uuid","year","month")
    
    val party   =  sqlContext.sql("select distinct uuid, year, month from dl_proj4_work.t_jtu_master_party")
    val account =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_account")
    val asset   =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_asset_round")
    val per_aum =  sqlContext.sql("select * from dl_proj4_work.v_FalconPersonalAUM_round")
    val hh_aum  =  sqlContext.sql("select * from dl_proj4_work.v_falconhhaum_round")
    val cash    =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_cash_round")
    val sec     =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_security_round")
    val invest  =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_invest_income_round")
    val SBL     =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_sbl_round")
    val mcall   =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_mcall_round")
    val mbal    =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_margin_balance_round")
    val tl      =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_tl_round")
    val mtg     =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_mortgage_round")
    val amex_pcm=  sqlContext.sql("select * from dl_proj4_work.FalconAmexPcm_ind")
    val elig    =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_subeligible")
    val pldg    =  sqlContext.sql("select * from dl_proj4_work.FalconXtrnlPldgVintage_ind")

    val joined_1 = party
                    .join(account,  common_attributes,  "Left_outer")
                    .join(asset,    common_attributes,  "Left_outer")
                    .join(per_aum,  common_attributes,  "Left_outer")
                    .join(hh_aum,   common_attributes,  "Left_outer")
                    .join(cash,     common_attributes,  "Left_outer")
                    .join(sec,      common_attributes,  "Left_outer")
                    .join(invest,   common_attributes,  "Left_outer")
                    .join(SBL,      common_attributes,  "Left_outer")
                    .join(mcall,    common_attributes,  "Left_outer")
                    .join(mbal,     common_attributes,  "Left_outer")
                    .join(tl,       common_attributes,  "Left_outer")
                    .join(mtg,      common_attributes,  "Left_outer")
                    .join(amex_pcm, common_attributes,  "Left_outer")
                    .join(elig,     common_attributes,  "Left_outer")
                    .join(pldg,     common_attributes,  "Left_outer")



    val common_attributes2 = Seq("uuid")

    val SB      =  sqlContext.sql("select * from dl_proj4_work.V_Falcon_LSB_Acct_ind")
    val emp     =  sqlContext.sql("select * from dl_proj4_work.V_FalconIsEmp_ind")
    val uhnw    =  sqlContext.sql("select * from dl_proj4_work.t_jtu_master_uhnw")

    val joined = joined_1
                    .join(SB,       common_attributes2, "Left_outer")
                    .join(emp,      common_attributes2, "Left_outer")
                    .join(uhnw,     common_attributes2, "Left_outer")

    sqlContext.sql("DROP TABLE IF EXISTS dl_proj4_work.t_jtu_master_combined")
    
    joined.write.format("csv").saveAsTable("dl_proj4_work.t_jtu_master_combined")
 
}
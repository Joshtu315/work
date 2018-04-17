#spark sql test
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

# run sql using scala 
def getCount():Unit={
val df=spark.sql("select count(distinct uuid) from dl_proj4_work.v_falconpartytoacctrole a join dl_proj4_work.v_falconasset b on a.keyuuid=b.keyuuid and a.year=b.dte_year and a.month=b.dte_month where a.roledesc in ('ACCOUNT OWNER')")
println(df.count())
} 


# sh
move file to hdfs 
module load hadoop/rrhadoop6 
hdfs dfs -copyFromLocal /v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/Freddie_Mac_Loan/historical_data1_Q11999.zip /wm/datalake/sandbox/PROJ4/work/josh/

# rm 
module load hadoop/rrhadoop6 
hdfs dfs -rm -r /wm/datalake/sandbox/PROJ4/work/josh/Asset_2015_Oct_Josh.csv

# rm skip trash 
module load hadoop/rrhadoop6 
hdfs dfs -rm -skipTrash /wm/datalake/sandbox/PROJ4/work/josh/Asset_2015_Oct_Josh.csv




# create view 
Create or replace view dl_proj4_work.josh_test as select * from dl_proj4_work.v_falconasset limit 5


#scala code to run sql 
http://spark.apache.org/docs/latest/api/python/pyspark.html


#############################################################################################################
# scala code to create tables 
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import java.util.UUID
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
val ss=SparkSession.builder().enableHiveSupport().getOrCreate()

def dropTempView(viewNames: String*): Unit = {
    if (viewNames.nonEmpty) {
      try {
        ss.catalog.dropTempView(viewNames.head)
      } catch {
        case e: RuntimeException => throw e
      }
      dropTempView(viewNames.tail: _*)
    }
  }
def persistToFile(df: Dataset[Row], fileName: String, format: String = "csv", local: Boolean = false): Unit = {
    if (local) {
      df.write.mode(SaveMode.Overwrite).option("header",false).option("delimiter","|").format(format).save(s"file://$fileName")
    } else {
      df.write.mode(SaveMode.Overwrite).option("header",false).option("delimiter","|").format(format).save(fileName)
    }
  }

def createTable(query:String,schemaName:String,tableName:String): Unit={
val df=ss.sql(query)
val tb=schemaName +"."+tableName
df.write.format("csv").saveAsTable(tb)

}


createTable("""select * from dl_proj4_work.V_FalconAcctToPartyRoleRaw_v2 limit 500""","dl_proj4_work","t_jtu_test10")


#############################################################################################################
%pyspark 
# output data into local 

import pandas as pd

def exporttocsv(query,filename):
    
    df = sqlContext.sql(query)

    names=[item[0] for item in df.dtypes]
    df1=pd.DataFrame(df.collect(),columns=names)

    df1.to_csv(filename)
    
exporttocsv("SELECT * FROM dl_proj4_work.V_FalconLAL_ConvertedPLA limit 100","/v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/PreAcxiom/InputTables/EDD/test10.csv")
#############################################################################################################

# scala to run sql 
a = sqlContext.sql(" select * from dl_pas_raw.ittm_mortgage").show(5000,false)


# create date based on month year 
select *,last_day(cast(CONCAT(YR,'-',MTH,'-','01') as date)) as date 
from dl_proj4_raw.keyacct_aum limit 5


# convert from string to date
select from_unixtime(unix_timestamp(dt2_business , 'MM/dd/yyyy')) from dl_proj4_work.V_FalconLAL_ConvertedPLA

/var/tmp/sigmund_bcpdwpas2




%pyspark

def loadData():
    df=sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").option("mode", "DROPMALFORMED").load("/wm/datalake/pas/raw/sinraman/201707426A_102015_04_PT1_10000.CSV")
    return df

def processData():
    df=loadData()
    print(type(df.select('TAPS4_SPEND').rdd.map(lambda r: r(0)).collect()))
    #Do processing here, As an example
    sum=df.rdd.map(lambda x: x.TAPS4_SPEND).reduce(lambda a,b: a+b if (a!=None and b!=None) else b)
    print(sum)

processData()
    

#config file path
\\v\region\na\appl\banking\pbg_risk_analytics\data\dev\sigmund\conf\zeppelin

# copy file to HDFS path
%sh 
module load hadoop/rrhadoop6 
hdfs dfs -mkdir /wm/datalake/sandbox/PROJ4/refinery/josh
hdfs dfs -copyFromLocal /v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/PreAcxiom/InputTables/MISSING_PARTY.csv /wm/datalake/sandbox/PROJ4/refinery/josh/


# load file 
val pla_mcall = spark
    .read
    .option("inferSchema", "false")
    .option("header", "true")
    .csv("/wm/datalake/sandbox/PROJ4/refinery/josh/NON_CONVERTED_PLA_MARGIN_V1.0.csv")

# load into hive
pla_mcall.write.format("ORC").saveAsTable("dl_proj4_raw.pla_mcall")




# query from Hive
%sql
select * from dl_proj4_raw.pla_mcall limit 5
	

# Read data and save to Hive
%spark 
#read file
val input_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").load("hdfs://sandbox.hortonworks.com:8020/user/zeppelin/yahoo_stocks.csv") 
#save to Hive
input_df.write.saveAsTable("%s.%s".format( "default" , "stockPrice" )) 

#Read back from Hive using SQL
%sql
Select * from stockPrice







var path="/wm/datalake/sandbox/PROJ4/refinery/josh/"
var file="NON_CONVERTED_PLA_MARGIN_V1.0.csv"
var db=
var table=

class loaddata {
	def load_edd (path: String, file:string, db: String, table: String ) {
		tempdata = spark
			.read
			.option("inferSchema", "true")
			.option("header", "true")
			.csv(path+file)
	}
	
	def load_hive ()
	{
	
	}
	

}



%scala
val flightData2015 = spark
.read
.option("inferSchema", "true")
.option("header", "true")
.csv("/mnt/defg/flight-data/csv/2015-summary.csv")

# check first 3 rows
flightData2015.take(3)

%python
flightData2015 = spark\
.read\
.option(“inferSchema”, “true”)\
.option(“header”, “true”)\
.csv("/mnt/defg/flight-data/csv/2015-summary.csv")

%scala
flightData2015.createOrReplaceTempView("flight_data_2015")
%python
flightData2015.createOrReplaceTempView("flight_data_2015")

%scala
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME""")

sqlWay.explain
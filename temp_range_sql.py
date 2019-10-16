import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark import Row
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()), 
    types.StructField('obstime', types.StringType()),])
    
    #reading input
    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView("sqlDf")
    #checking for qflag and filtering observation having TMAX and TMIN
    correctObservation = spark.sql("SELECT * FROM sqlDf WHERE qflag IS NULL and observation IN ('TMAX','TMIN')")
    correctObservation.createOrReplaceTempView("correctObservation")
    #creating temp dataframe to keep data having both tmax and tmin
    temp = spark.sql("SELECT date, station,  COUNT(*) AS count FROM correctObservation GROUP BY date,station")
    temp.createOrReplaceTempView("temp")
    temp1=spark.sql("SELECT * FROM temp WHERE count>1")
    temp1.createOrReplaceTempView("temp1")
    newObservation=spark.sql("SELECT temp1.date,temp1.station,observation,value FROM temp1 INNER JOIN correctObservation ON temp1.date = correctObservation.date AND temp1.station = correctObservation.station")
    newObservation.createOrReplaceTempView("newObservation")
    #calculating max temperature for each station
    addMax=spark.sql("SELECT date,station,MAX(value) AS max FROM newObservation GROUP BY date,station")
    addMax.createOrReplaceTempView("addMax")
    #calculating min temperature for each station
    addMin=spark.sql("SELECT date,station,MIN(value) AS min FROM newObservation GROUP BY date,station")
    addMin.createOrReplaceTempView("addMin")
    #joining min and max columns 
    minMax=spark.sql("SELECT addMax.date,addMax.station,max,min FROM addMax INNER JOIN addMin on addMax.date=addMin.date AND addMax.station=addMin.station")
    minMax.createOrReplaceTempView("minMax")
    #dividing by 10 to convert into celsius
    celsiusData=spark.sql("SELECT date,station,max/10 AS max,min/10 AS min FROM minMax")
    celsiusData.createOrReplaceTempView("celsiusData")
    #calculatind difference between max and min temperatures
    rangeData=spark.sql("SELECT date,station,round(max-min,2) AS diff from celsiusData")
    rangeData.createOrReplaceTempView("rangeData")
    #calculating max difference for each date
    maxDiff=spark.sql("SELECT date,MAX(diff) AS diff FROM rangeData GROUP BY date")
    maxDiff.createOrReplaceTempView("maxDiff")
    #selecting station having max difference for each date
    station = spark.sql("SELECT rangeData.date,rangeData.station,rangeData.diff FROM maxDiff INNER JOIN rangeData on maxDiff.date=rangeData.date AND maxDiff.diff=rangeData.diff ORDER BY date")
    #writing output to csv file
    station.write.csv(output,mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    #inputs="weather-1"
    #output="SQLOutput" 
    main(inputs, output)

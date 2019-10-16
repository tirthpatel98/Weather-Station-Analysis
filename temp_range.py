import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

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
    #checking for qflag and filtering observation having TMAX and TMIN
    correctObservation=weather.where((weather['qflag'].isNull()) & (weather['observation'].isin(['TMAX','TMIN']))).sort('date','station')
    #creating temp dataframe to keep data having both tmax and tmin
    temp=correctObservation.groupBy('date','station').count()
    temp1=temp.where(temp['count']>1)
    newObservation = functions.broadcast(temp1).join(correctObservation,['date','station']).cache()
    #calculating max temperature for each station
    addMax=newObservation.groupBy('station','date').agg(functions.max(newObservation['value']).alias('maxT'))
    #calculating min temperature for each station
    addMin=newObservation.groupBy('station','date').agg(functions.min(newObservation['value']).alias('minT'))
    #joining min and max columns 
    minMax = addMax.join(addMin,['date','station'])
    #dividing by 10 to convert into celsius
    celsiusData=minMax.select(minMax['date'],minMax['station'],minMax['maxT']/10,minMax['minT']/10).cache()
    celsiusData=celsiusData.withColumnRenamed('(maxT / 10)','maxT').withColumnRenamed('(minT / 10)','minT')
    #calculatind difference between max and min temperatures
    rangeData=celsiusData.withColumn('diff',celsiusData['maxT']-celsiusData['minT']).cache()
    #calculating max difference for each date
    maxDiff=rangeData.groupBy('date').agg(functions.max('diff').alias('diff')).sort('date')
    station=maxDiff.join(rangeData,['date','diff']).sort('date')
    #selecting station having max difference for each date
    maxTemDiffStation = station.select(station['date'],station['station'],functions.round(station['diff'],2).alias('diff'))
    #writing output to csv file
    maxTemDiffStation.write.csv(output,mode='overwrite')
    
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    #inputs="weather-1"
    #output="RangeOutput" 
    main(inputs, output)

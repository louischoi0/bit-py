from pymongo import MongoClient
from pyspark import *
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import pyspark.sql.types as T

import time

import sys
from datetime import datetime


class ioExecutor :
    def __init__(self,dbname) :
        self.client = MongoClient("localhost",27017)
        self.db = self.client[dbname]
        
    def initcollection(self,collectionname) :
        self.collection = self.db[collectionname]

    def find(self,query) :
        return self.collection.find(query)


class strategyComponent :
    def __init__(self) :
        pass
        
    def preevent(self) :
        pass

#class momentum(strategyComponent) :
#    def 


class tickerStater :

    def __init__(self,sparksession) :
        self.spark = sparksession
        self.tdf = None

    def initdf(self,record,unit) :
        record = self.preprocessing(record)

        def obj(x) :
            return x.split("!")[1]

        obj = F.udf(obj,T.StringType())

        self.tdf = self.spark.sparkContext.parallelize(record).toDF()\
                .withColumn("unit",obj("_id"))

        self.tdf = self.tdf.filter( col("unit") == unit )
        self.tdf.show() 

        self.tdf = self.tdf.drop("site","unit") 

    def preprocessing(self,record) :
        for r in record :
            v = r["candleDateTime"]
            dt = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
            dtt = time.mktime(dt.timetuple())
            r["timestamp"] = dtt

        def netlambda(series) :
            series = sorted(series,key=lambda x : x[0])
            return ( (series[-1][1] - series[0][1]) / series[0][1] ) * 100

        self.netlambda = F.udf(netlambda,T.DoubleType())

        def isum(series) :
            return sum(x[1] for x in series)

        self.sum = F.udf(isum,T.DoubleType())

        return record

    def groupbytimespan(self,span,fd,typ=T.DoubleType) :
        _tdf = self.tdf.withColumn("timespan", col("timestamp") / (60 * span))
        _tdf = _tdf.withColumn("timespan", _tdf["timespan"].cast(T.IntegerType()))

#        return _tdf.groupBy("timespan").agg(F.collect_list(F.struct("timestamp",fd)).alias("tupleseries"))
        return self.collectintimespan(_tdf,"timespan",fd)

    def collectintimespan(self,df,keyfield,valuefield) :
        return df.select(keyfield,F.struct("timestamp",valuefield))\
                .rdd.map(lambda x : ( x[0] , [ x[1] ] ) )\
                .reduceByKey(lambda x,y : x + y)\
                .toDF()\
                .select(ascol("_1",keyfield), 
                        ascol("_2","tupleseries"))

    def actioningrouped(self,grouped,action) :
        return grouped.withColumn("result",action("tupleseries"))\
                .select("timespan","result")
   
    def action(self,span,fd,action,typ=T.DoubleType) :
        _df = self.groupbytimespan(span,fd,typ)
        return self.actioningrouped(_df,action)

    def run(self,record) :
        self.initdf(record,"1")
        _volumedf = self.action(180,"candleAccTradeVolume",self.sum)
        sys.exit(1)

        _volumedf = self.aselect(_volumedf,"volume")
        _volumedf.show()

        res = self.action(180,"tradePrice",self.netlambda)
        res.show()

        res = self.retrvtimestamp(res,180)
        res = res.sort(col("result").desc())

        res = res.join(_volumedf, _volumedf._timespan == res.timespan)\
                .drop("_timespan")

        tsps = self.taketimespans(res,15)
        res = self.timespanaheadslide(res,tsps,10)
    
        self.showall(res) 
        sys.exit(1)

        fres = res.filter( col("result") > 1 )

        fres.show(100)
        
        _res = fres.sort( col("timespan").desc())

        _res.show() 

        print(res.count())
        print(fres.count())


    def aselect(self,df,name) :
        return df.select(ascol("timespan","_timespan"),
                         ascol("result",name))

    def retrvtimestamp(self,tdf,minunit) :
        _tdf = tdf.withColumn("timestamp" , col("timespan") * 60 * minunit)
        return _tdf.withColumn("candleDateTime" , F.from_unixtime("timestamp").cast(T.StringType()))\
                .sort(col("timestamp").desc())

    def ahead(self,actiondf,timespan,count) :
        return actiondf.filter(col("timespan") <= timespan)\
                .filter(col("timespan") > (timespan - count))\
                .sort(col("timespan"))

    def timespanaheadslide(self,actiondf,timespans,count) :
        return list(self.ahead(actiondf,t,count) for t in timespans)

    def taketimespans(self,actiondf,count) :
        return list(row.timespan for row in actiondf.take(count))
   
    def showall(self,dfs) :
        for d in dfs :
            d.show()

def ascol(c,a) :
    return col(c).alias(a)

if __name__ == "__main__" :

    spark = SparkSession.builder.getOrCreate()
    ts = tickerStater(spark)

    io = ioExecutor("bit-core")
    io.initcollection("bitts")
    r = io.find({ "code" :"BTC" })
    r = list(r) 

    ts.run(r)

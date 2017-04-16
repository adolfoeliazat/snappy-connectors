## 5 Minutes Quick Start Guide

In this quick start guide, you will learn how to configure SnappyData cluster to use SnappyData
GemFire Connector functionalities.

### Prerequisites

Before you start, you should have basic knowledge of GemFire and Spark. 

You need 2 terminals to follow along, one for GemFire shell `gfsh`, and one for staring SnappyData cluster. Set up Jdk 1.8 on both of them.

### GemFire `gfsh` terminal
In this terminal, start GemFire cluster, deploy SnappyData GemFire Connector's gemfire-function jar, and create demo regions.
Lets assume that the domain class objects being stored in GemFire region is present in a jar , person.jar , containing Person.class 

gfsh
gfsh>start locator --name=locator1 --port=55221  
--classpath=/snappydata/downloads/loader/person.jar

gfsh>start server --name=server1 
--locators=localhost[55221] --server-port=0 
--classpath=/snappydata/downloads/loader/person.jar

gfsh>start server --name=server2 
--locators=localhost[55221] --server-port=0 
--classpath=/snappydata/downloads/loader/person.jar
```

Then create two demo regions:
```
gfsh>create region --name=gemTable1 --type=PARTITION --key-constraint=java.lang.String --value-constraint=java.lang.String

gfsh>create region --name=gemTable2 --type=PARTITION --key-constraint=java.lang.Integer --value-constraint=load.Person

```

Deploy SnappyData GemFire Connector's gemfire-function jar (`gemfire-functions_2.11-0.5.0.jar`):
```
gfsh>deploy --jar=`<path to jar>`/gemfire-functions_2.11-0.5.0.jar


### Configuring the snappydata cluster
The snappydata cluster needs to be configured so that it can talk to GemFire cluster.
To do that, modify the servers and leads configuration file at locations 
`<SnappyData-Home>`/conf/leads
and 
`<SnappyData-Home>`/conf/servers
to add the snappydata-gemfire connector jar (gemfire-spark-connector_2.11-0.5.0.jar) and the person.jar in the classpath & to set the remote GemFire Cluster locator 
The servers file should look like

localhost -locators=localhost:10334 -client-bind-address=localhost -client-port=1528 -heap-size=20g  
-classpath=`<path-to-jar>`gemfire-spark-connector_2.11-0.5.0.jar:`<path-to-jar>`/person.jar  
-remote-locators=localhost[55221] 

Similar configuartion needs to be provided in 'leads' file
```

In order to enable GemFire specific functions, you need to import 
`import io.snappydata.spark.gemfire.connector._`
```
scala> import io.snappydata.spark.gemfire.connector._
```

### Save Pair RDD to GemFire
In the Spark shell, create a simple pair RDD and save it to GemFire:
```
scala> val data = Array(("1", "one"), ("2", "two"), ("3", "three"))
data: Array[(String, String)] = Array((1,one), (2,two), (3,three))

scala> val distData = sc.parallelize(data)
distData: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[0] at parallelize at <console>:14

scala> distData.saveToGemFire("gemTable1")
15/02/17 07:11:54 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:29, took 0.341288 s
```

Verify the data is saved in GemFire using `gfsh`:
```
gfsh>query --query="select key,value from /gemTable1.entries"

Result     : true
startCount : 0
endCount   : 20
Rows       : 3

key | value
--- | -----
1   | one
3   | three
2   | two

NEXT_STEP_NAME : END
```

### Save Non-Pair RDD to GemFire 
Saving non-pair RDD to GemFire requires an extra function that converts each 
element of RDD to a key-value pair. Here's sample session in Spark shell:
```
scala> val data2 = Array("a","ab","abc")
data2: Array[String] = Array(a, ab, abc)

scala> val distData2 = sc.parallelize(data2)
distData2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:17

scala> distData2.saveToGemFire("gemTable1", e => (e.length, e))
[info 2015/02/17 12:43:21.174 PST <main> tid=0x1]
...
15/02/17 12:43:21 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:52, took 0.251194 s
```

Verify the result with `gfsh`:
```
gfsh>query --query="select key,value from /gemTable1.entrySet"

Result     : true
startCount : 0
endCount   : 20
Rows       : 3

key | value
--- | -----
2   | ab
3   | abc
1   | a

NEXT_STEP_NAME : END

```

### Expose GemFire Region As RDD
The same API is used to expose both replicated and partitioned region as RDDs. 
```
scala> val rdd = sc.gemfireRegion[String, String]("gemTable1")


scala> rdd.foreach(println)
(1,one)
(3,three)
(2,two)


Note: use the right type of region key and value, otherwise you'll get
ClassCastException. 


Next: [Loading Data from GemFire](3_loading.md)

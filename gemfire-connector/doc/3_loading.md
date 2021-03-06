
### Expose GemFire Region As RDD
To expose full data set of a GemFire region as a Spark RDD, call `gemfireRegion` method on the SparkContext object.
The same API is used to expose both replicated and partitioned region as RDDs. 

```
scala> val rdd = sc.gemfireRegion[String, String]("gemTable1")

scala> rdd.foreach(println)
(1,one)
(3,three)
(2,two)
```

Note: use the right type of region key and value, otherwise you'll get
ClassCastException. 

### Expose GemFire Region Containing Rows ( GemFireRows) As An External Table
```
val externalBsegTable = snc.createExternalTable("bsegInGem", 
     "org.apache.spark.sql.sources.connector.gemfire.DefaultSource", schema,
     Map[String, String]("regionPath" -> "bseg1", "valueClass" -> "org.apache.spark.sql.Row"))
```     
A GemFire region which  stores or will store Row objects, can be made avaialble to snappydata as an external table. 
Here the schema is the StructType defining the fields of the Row object being stored in the Region.

If the external table should contain Primary Key Column as well as the Row, then the API would be

```
val externalBsegTable = snc.createExternalTable("bsegInGem",
   "org.apache.spark.sql.sources.connector.gemfire.DefaultSource", keyPlusValueSchema,
   Map[String, String]("regionPath" -> "bseg1", "valueClass" -> "org.apache.spark.sql.Row",
   "keyClass" -> "java.lang.Long"     
     ))
```
In the above case, since Key is also intended to be part of the table, the keyPlusValueSchema should include first StructField as the Primary Key Column followed by the Value schema & keyClass should indicate the type of the Primary Key being used to store the Row in GemFire

### Expose GemFire Region Containing JavaBeans compliant objects  As An External Table
```
snc.createExternalTable(externalPersonsTable1,      "org.apache.spark.sql.sources.connector.gemfire.DefaultSource", Map[String, String]("regionPath" -> personsRegionName, "valueClass" -> "load.Person"))
```

If the GemFire Region contains JavaBeans compliant objects, then specifying valueClass as the fully qualified class name of the Bean object, enables the region to be exposed as an external table.
The getter methods would be the field names of the columns ( for eg getId would become a column with name id)
If the table has to include key along with the value, then  keyClass needs to be passed as options ( for eg "keyClass" -> "java.lang.Integer")

### Create An External Table Based On A DataFrame And Store The Data In The Region

```
bsegDF.write.format("org.apache.spark.sql.sources.connector.gemfire.DefaultSource").  
      option("regionPath", "bseg1").
     option("primaryKeyColumnName", "id1").
     option("valueClass", "org.apache.spark.sql.Row").saveAsTable("bsegTable")
```
In the above example, a DataFrame is used to create an external table bsegTable, with the schema same as the DataFrame bsegDF. The primary key column name should be specified for this to work correctly. In the above example, it is assumed that the dataframe contains a column "id1"
 
## GemFire RDD Partitions

GemFire has two region types: **replicated**, and
**partitioned** region. Replicated region has full dataset on
each server, while partitioned region has its dataset spanning
upon multiple servers, and may have duplicates for high 
availability.

Since replicated region has its full dataset available on every
server, there is only one RDD partition for a `GemfireRegionRDD` that 
represents a replicated region.

For a `GemFireRegionRDD` that represents a partitioned region, there are 
many potential  ways to create RDD partitions. So far, we have 
implemented ServerSplitsPartitioner, which will split the bucket set
on each GemFire server into two RDD partitions by default.
The number of splits is configurable, the following shows how to set 
three partitions per GemFire server:

```
import io.snappydata.spark.gemfire.connector._

val opConf = Map(PreferredPartitionerPropKey -> ServerSplitsPartitionerName,
                 NumberPartitionsPerServerPropKey -> "3")

val rdd1 = sc.gemfireRegion[String, Int]("gemTable1", opConf = opConf)
 
```


## GemFire Server-Side Filtering
Server-side filtering allow exposing partial dataset of a GemFire region
as a RDD, this reduces the amount of data transferred from GemFire to 
Spark to speed up processing.

```
val rdd = sc.gemfireRegion("<region path>").where("<where clause>")
```

The above call is translated to OQL query `select key, value from /<region path>.entries where <where clause>`, then 
the query is executed for each RDD partition. Note: the RDD partitions are created the same way as described in the 
section above.

In the following demo, javabean class `Emp` is used, it has 5 attributes: `id`, `lname`, `fname`, `age`, and `loc`. 
In order to make `Emp` class available on GemFire servers, we need to deploy a jar file that contains `Emp` class, 
now build the `emp.jar`,  deploy it and create region `emps` in `gfsh`:

```
gfsh
gfsh> deploy --jar=<path to connector project>/emp.jar
gfsh> create region --name=emps --type=PARTITION 
```

Now in SnappyData shell, generate some random `Emp` records, and save them to region `emps` (remember to add `emp.jar` to 
SnappyData shell classpath before starting SnappyData shell):

```
import io.snappydata.spark.gemfire.connector._
import scala.util.Random
import demo.Emp

val lnames = List("Smith", "Johnson", "Jones", "Miller", "Wilson", "Taylor", "Thomas", "Lee", "Green", "Parker", "Powell")
val fnames = List("John", "James", "Robert", "Paul", "George", "Kevin", "Jason", "Jerry", "Peter", "Joe", "Alice", "Sophia", "Emma", "Emily")
val locs = List("CA", "WA", "OR", "NY", "FL")
def rpick(xs: List[String]): String = xs(Random.nextInt(xs.size))

val d1 = (1 to 20).map(x => new Emp(x, rpick(lnames), rpick(fnames), 20+Random.nextInt(41), rpick(locs))).toArray
val rdd1 = sc.parallelize(d1) 
rdd1.saveToGemFire("emps", e => (e.getId, e))
```

Now create a RDD that contains all employees whose age is less than 40, and display its contents:

```
val rdd1s = sc.gemfireRegion("emps").where("value.getAge() < 40")

rdd1s.foreach(println)
(5,Emp(5, Taylor, Robert, 32, FL))
(14,Emp(14, Smith, Jason, 28, FL))
(7,Emp(7, Jones, Robert, 26, WA))
(17,Emp(17, Parker, John, 20, WA))
(2,Emp(2, Thomas, Emily, 22, WA))
(10,Emp(10, Lee, Alice, 31, OR))
(4,Emp(4, Wilson, James, 37, CA))
(15,Emp(15, Powell, Jason, 34, NY))
(3,Emp(3, Lee, Sophia, 32, OR))
(9,Emp(9, Johnson, Sophia, 25, OR))
(6,Emp(6, Miller, Jerry, 30, NY))
```
## Exposing GemFire Region as an External Table in SnappyData
If the GemFire region contains key - value pairs of appropriate types, it is possible to expose GemFireRegion as an external Table in SnappyData.
If GemFire region's key & values are of types:

1) well known java types like Date, Integer, Long Timestamp etc where it is possible to map the Java types to known SQL Types

2) Classes confirming to Java Bean specifications.

```
case when Region contains JavaBean objects.
val df = snc.sql(s"CREATE EXTERNAL TABLE GEMTABLE USING org.apache.spark.sql.sources.connector.gemfire.DefaultSource OPTIONS(regionPath 'gemTable',keyClass 'java.lang.Integer', valueClass 'load.Person')")

here specification of valueClass as load.Person & keyClass as java.lang.Integer allows us to convert the objects into Row. 
The table would contain both key & value as part of schema. If we want to exclude either Key or Value type from schema, do not pass that option.

```

3) GemFire region where data was stored using SnappyData api for saving DataFrames.
In this case, we need to pass the total schema definition which contains both key & value .
In case we do not want to include key portion as part of schema, then we need not pass the keyClass as an option. 
```
val df = snc.read.format("org.apache.spark.sql.sources.connector.gemfire.DefaultSource").
     option("regionPath", "gemTable1").
     option("keyClass", "java.lang.Long").
     option("valueClass", "org.apache.spark.sql.Row").
     schema(StructType(Array(StructField("keyid", LongType, false), StructField("id", LongType, false), StructField("sym", StringType, true)))).load
```

In all such cases, it is possible to deduce the schema & thus represent the Region as an external table.



Next: [RDD Join and Outer Join GemFire Region](4_rdd_join.md)

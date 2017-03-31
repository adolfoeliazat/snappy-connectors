/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt._
import sbt.Keys._
import scoverage.ScoverageSbtPlugin._
import scoverage.ScoverageSbtPlugin

object GemFireSparkConnectorBuild extends Build {
  import Settings._
  import Dependencies._ 

  lazy val root = Project(
    id = "root", 
    base =file("."), 
    aggregate = Seq(gemfireFunctions, gemfireSparkConnector),
    settings = commonSettings ++ Seq( 
     name := "GemFire Connector for Apache Spark",
externalResolvers <<= externalResolvers map {rs =>
gfcResolvers ++ rs
},

     publishArtifact :=  false,
     publishLocal := { },
     publish := { }
    )
  ).dependsOn( gemfireSparkConnector, gemfireFunctions)

  lazy val sharedConnectorProj = Project(
    id = "shared-connector",
    base = file("shared"),
    settings = commonSettings ++ Seq(libraryDependencies ++= Dependencies.connector,
    //externalResolvers ++= gfcResolverss,
    externalResolvers <<= externalResolvers map {rs =>
  gfcResolvers ++ rs
},
    description := "Shared artifacts",
    target := file("shared/connector/classes"),
    publishArtifact :=  false,
    publishLocal := { },
    publish := { }

    )
  )
 
  lazy val sharedFunctionsProj = Project(
    id = "shared-functions",
    base = file("shared"),
    settings = commonSettings ++ Seq(libraryDependencies ++= Dependencies.functions,
    //externalResolvers ++= gfcResolverss,

externalResolvers <<= externalResolvers map {rs =>
gfcResolvers ++ rs
},
    description := "Shared artifacts",
    target := file("shared/gfefunctions/classes"),
    publishArtifact :=  false,
    publishLocal := { },
    publish := { }
    )
  )
  
 lazy val gemfireFunctions = Project(
    id = "gemfire-functions",
    base = file("gfeFunctions"),
    settings = commonSettings ++ Seq(libraryDependencies ++= Dependencies.functions,
    //externalResolvers ++= gfcResolverss,

externalResolvers <<= externalResolvers map {rs =>
gfcResolvers ++ rs
},
    description := "Required GemFire Functions to be deployed onto the GemFire Cluster before using the GemFire Spark Connector"
    )
  ).settings((mappings in (Configurations.Compile, packageBin)) <++= mappings in (sharedFunctionsProj, Configurations.Compile,packageBin )
).dependsOn(sharedFunctionsProj).configs(IntegrationTest)
  
  lazy val gemfireSparkConnector = Project(
    id = "gemfire-spark-connector",
    base = file("connector"),
    settings = gfcSettings ++ Seq(libraryDependencies ++= Dependencies.connector,
    //externalResolvers ++= gfcResolverss,

externalResolvers <<= externalResolvers  map {rs =>
gfcResolvers ++ rs
},
    description := "A library that exposes GemFire regions as Spark RDDs, writes Spark RDDs to GemFire regions, and executes OQL queries from Spark Applications to GemFire"
    )).settings((mappings in (Configurations.Compile, packageBin)) <++= mappings in (sharedConnectorProj, Configurations.Compile,packageBin )
 ).dependsOn(sharedConnectorProj) .configs(IntegrationTest)

 
  /******** Demo Project Definitions ********/ 
  lazy val demoPath = file("gemfire-spark-demos")

  lazy val demos = Project ( 
    id = "gemfire-spark-demos",
    base = demoPath,
    settings = demoSettings,
    aggregate = Seq(basicDemos)
  )
 
  lazy val basicDemos = Project (
    id = "basic-demos",
    base = demoPath / "basic-demos",
    settings = demoSettings ++ Seq(libraryDependencies ++= Dependencies.demos,
     // externalResolvers ++= gfcResolverss,

externalResolvers <<= externalResolvers map {rs =>
gfcResolvers ++ rs
},
      description := "Sample applications that demonstrates functionality of the GemFire Spark Connector"
    )
  ).dependsOn(gemfireSparkConnector)
}


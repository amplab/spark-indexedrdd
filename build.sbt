name := "spark-indexedrdd-test"

version := "0.3"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

spName := "amplab/spark-indexedrdd-test"

sparkVersion := "1.5.0"

sparkComponents += "core"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

resolvers += "Spark Dave" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "amplab" % "spark-indexedrdd" % "0.3"

// Run tests with more memory
javaOptions in test += "-Xmx2G"

fork in test := true

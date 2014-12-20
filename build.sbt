import xerial.sbt.Sonatype.SonatypeKeys
import SonatypeKeys._

xerial.sbt.Sonatype.sonatypeSettings

name := "spark-indexedrdd"

version := "0.1-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/amplab/spark-indexedrdd</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:amplab/spark-indexedrdd.git</url>
    <connection>scm:git:git@github.com:amplab/spark-indexedrdd.git</connection>
  </scm>
  <developers>
    <developer>
      <id>ankurdave</id>
      <name>Ankur Dave</name>
      <url>https://github.com/ankurdave</url>
    </developer>
  </developers>)

// Enable Junit testing.
// libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

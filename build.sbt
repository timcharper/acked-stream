name := "acked-streams"

organization := "com.timcharper"

version := "1.0-RC1"

author := "Tim Harper"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "org.scalatest"     %% "scalatest"                % "2.2.1" % "test")

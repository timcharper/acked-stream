name := "acked-streams"

organization := "com.timcharper"

version := "2.0-M1"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7", "2.10.5")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0-M1",
  "org.scalatest"     %% "scalatest"                % "2.2.1" % "test")

homepage := Some(url("https://github.com/timcharper/acked-stream"))

licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

pomExtra := {
  <scm>
    <url>https://github.com/timcharper/acked-stream</url>
    <connection>scm:git:git@github.com:timcharper/acked-stream.git</connection>
  </scm>
  <developers>
    <developer>
      <id>timcharper</id>
      <name>Tim Harper</name>
      <url>http://timcharper.com</url>
    </developer>
  </developers>
}

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

name := "acked-streams"

organization := "com.timcharper"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8", "2.12.1")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"  % "2.4.16",
  "org.scalatest"     %% "scalatest"    % "3.0.1" % "test")

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

import sbt.Keys._
import sbt._

object Build extends Build {

  val AppVersion = "0.0.1-SNAPSHOT"
  val ScalaVersion = "2.10.4"

  lazy val main = Project("mrcube", file("."), settings = defaultSettings)
    .settings(organization := "in.ashwanthkumar",
      version := AppVersion,
      libraryDependencies ++= Seq(
        "com.twitter" %% "scalding-core" % "0.12.0" % "provided",
        "org.apache.hadoop" % "hadoop-core" % "1.2.1" % "provided",
        "org.scalatest" %% "scalatest" % "2.2.0" % "test"
      )
  )

  lazy val defaultSettings = super.settings ++ Seq(
    fork in run := false,
    parallelExecution in This := true,
    publishMavenStyle := true,
    crossPaths := true,
    publishArtifact in Test := false,
    publishArtifact in(Compile, packageDoc) := true,
    publishArtifact in(Compile, packageSrc) := true,
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := _pomExtra
  )

  val _pomExtra =
    <url>http://github.com/ashwanthkumar/mrcube</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:ashwanthkumar/mrcube.git</url>
        <connection>scm:git:git@github.com:ashwanthkumar/mrcube.git</connection>
      </scm>
      <developers>
        <developer>
          <id>ashwanthkumar</id>
          <name>Ashwanth Kumar</name>
          <url>http://www.ashwanthkumar.in/</url>
        </developer>
      </developers>

}

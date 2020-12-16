name := "moex"

version := "0.1"

scalaVersion := "2.12.8"

sbtPlugin := true

ThisBuild / scalafmtOnCompile := true

assemblyJarName in assembly := "moex.jar"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.0",
  "org.scala-lang"  % "scala-compiler"  % "2.12.8",
  "org.scala-sbt" % "sbt" % "1.2.7" % "provided",
  "org.slf4j" % "slf4j-nop" % "1.7.25",
  "org.scalatest" %% "scalatest"  % "3.0.5" % Test,
  "io.circe" %% "circe-parser" % "0.12.3",
  "com.opencsv" % "opencsv" % "4.1"
)

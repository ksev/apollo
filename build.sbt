name := "apollo"

version := "0.0.0"

scalaVersion := "2.10.2"

scalacOptions ++= Seq(
    "-feature", 
    "-deprecation",
    "-language:postfixOps",
    "-language:implicitConversions"
)

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.2.0",
    "com.typesafe" % "config" % "1.0.2",
    "com.typesafe.akka" %% "akka-testkit" % "2.2.0" % "test",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)

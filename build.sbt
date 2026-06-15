ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "backpac_de_assignment",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.6" % "provided",
      "org.apache.spark" %% "spark-hive" % "3.5.6" % "provided"
    )
  )

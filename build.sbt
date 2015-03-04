lazy val commonSettings = Seq(
  name := "squall",
  organization := "ch.epfl.data",
  version := "0.2.0",
  scalaVersion := "2.11.5"
)

lazy val core = (project in file("core")).
  // TODO: add java options
  settings(commonSettings: _*).
  settings(
    javacOptions ++= Seq(
      "-target", "1.7",
      "-source", "1.7"),
    // We need to add Clojars as a resolver, as Storm depends on some
    // libraries from there.
    resolvers += "clojars" at "https://clojars.org/repo",
    libraryDependencies ++= Seq(
      "org.apache.storm" % "storm-core" % "0.9.3",
      "net.sf.trove4j" % "trove4j" % "3.0.2",
      "net.sf.jsqlparser" % "jsqlparser" % "0.7.0",
      // TODO: some problem with thrift:
      //"storm" % "libthrift7" % "0.7.0-2",
      "org.apache.thrift" % "libthrift" % "0.9.2",
      "com.sleepycat" % "je" % "5.0.73",
      "ujmp-complete" % "ujmp-complete" % "0.2.4",
      "au.com.bytecode" % "opencsv" % "2.4",
      "com.google.guava" % "guava" % "13.0"
    )
  )

lazy val frontend = (project in file("frontend")).
  dependsOn(core, frontend_core).
  settings(commonSettings: _*).
  settings(
    name := "squall-frontend",
    libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _)
  )

lazy val frontend_core = (project in file("frontend-core")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    name := "squall-frontend-core",
    libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _)
  )

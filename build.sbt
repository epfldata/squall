import complete.DefaultParsers._

scalaVersion := "2.11.6"

lazy val runParser = inputKey[Unit]("Runs the SQL interface with the given configuration.")
lazy val runPlanner = inputKey[Unit]("Runs the imperative interface with the given configuration.")

lazy val commonSettings = Seq(
  name := "squall",
  organization := "ch.epfl.data",
  version := "0.2.0",
  scalaVersion := "2.11.6",
  // Avoids having the scala version in the path to the jars
  crossPaths := false,
  // Options for assembling a single jar
  test in assembly := {},
  assemblyJarName in assembly := name.value + "-standalone-" + version.value + ".jar",
  assemblyJarName in assemblyPackageDependency := name.value + "-dependencies-" + version.value + ".jar",
  // TODO: this is very wrong, I'm taking the default strategy, and instead of
  // using MergeStrategy.deduplicate, I'm using MergeStrategy.first to fix the
  // conflicts
  assemblyMergeStrategy in assembly := {
        case x if Assembly.isConfigFile(x) =>
          MergeStrategy.concat
        case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
          MergeStrategy.rename
        case PathList("META-INF", xs @ _*) =>
          (xs map {_.toLowerCase}) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
              MergeStrategy.discard
            case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
              MergeStrategy.discard
            case "plexus" :: xs =>
              MergeStrategy.discard
            case "services" :: xs =>
              MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
              MergeStrategy.filterDistinctLines
            case _ => MergeStrategy.first
          }
        case _ => MergeStrategy.first
      }
)

lazy val squall = (project in file("squall-core")).
  settings(commonSettings: _*).
  settings(
    javacOptions ++= Seq(
      "-target", "1.7",
      "-source", "1.7"),
    mainClass := Some("ch.epfl.data.squall.main.Main"),
    unmanagedSourceDirectories in Compile += baseDirectory.value / "../squall-examples/squall-java-examples/src/",
    // Don't use scala as a dependency
    //autoScalaLibrary := false,
    // Set the external library directories to ./contrib
    unmanagedBase := baseDirectory.value / "../contrib",
    // We need to add Clojars as a resolver, as Storm depends on some
    // libraries from there.
    resolvers += "clojars" at "https://clojars.org/repo",
    libraryDependencies ++= Seq(
      // Versions that were changed when migrating from Lein to sbt are
      // commented just before the library
      "net.sf.jsqlparser" % "jsqlparser" % "0.7.0",
      "net.sf.trove4j" % "trove4j" % "3.0.2",
      "net.sf.opencsv" % "opencsv" % "2.3",
      // bdb-je: 5.0.84 -> 5.0.73
      "com.sleepycat" % "je" % "5.0.73",
      // storm-core: 0.9.2-incubating -> 0.9.3
      "org.apache.storm" % "storm-core" % "0.9.3" % "provided",
      "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
      //"io.dropwizard" % "dropwizard-metrics" % "0.8.1",
      //"org.apache.storm" % "storm-starter" % "0.9.3",
      "junit" % "junit" % "4.12" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "org.apache.hadoop" % "hadoop-client" % "2.2.0" exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" exclude("org.slf4j", "slf4j-log4j12")
      //"com.github.ptgoetz" % "storm-signals" % "0.2.0",
      //"com.netflix.curator" % "curator-framework" % "1.0.1"
    ),
    // http://www.scala-sbt.org/0.13/docs/Running-Project-Code.html
    // We need to fork the JVM, as storm uses multiple threads
    fork := true,
    // Running tasks
    runParser := {
      val arguments: Seq[String] = spaceDelimited("<arg>").parsed
      val classpath: Seq[File] = (
        ((fullClasspath in Compile).value map { _.data }) ++
          (arguments.tail map { file(_) })
      )
      val options = ForkOptions(
        bootJars = classpath,
        workingDirectory = Some(file("./bin"))
      )
      val mainClass: String = "ch.epfl.data.squall.api.sql.main.ParserMain"
      val exitCode: Int = Fork.java(options, mainClass +: arguments)
      sys.exit(exitCode)
    },
    runPlanner := {
      val arguments: Seq[String] = spaceDelimited("<arg>").parsed
      val classpath: Seq[File] = (
        ((fullClasspath in Compile).value map { _.data }) ++
          (arguments.tail map { file(_) })
      )
      val options = ForkOptions(
        bootJars = classpath,
        workingDirectory = Some(file("./bin"))
      )
      val mainClass: String = "ch.epfl.data.squall.main.Main"
      val exitCode: Int = Fork.java(options, mainClass +: arguments)
      sys.exit(exitCode)
    },
    // Testing
    libraryDependencies +=  "org.scalatest" % "scalatest_2.11" % "2.2.4" % Test
  )

// For the macros
lazy val functional_macros = (project in file("squall-functional")).
  dependsOn(squall).
  settings(commonSettings: _*).
  settings(
    name := "squall-frontend-macros",
    libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
    scalaSource in Compile := baseDirectory.value / "macros",
    target := target.value / "macros"
  )


// Temporal directory for REPL output
// TODO: this should probably be a task
lazy val repl_outdir   = sbt.IO.createTemporaryDirectory
lazy val repl_classdir = {sbt.IO.createDirectory(repl_outdir / "classes"); repl_outdir / "classes"}

lazy val functional = (project in file("squall-functional")).
  dependsOn(squall, functional_macros).
  settings(commonSettings: _*).
  settings(
//    fork := true,
    // TODO: this is only necessary because we are using the .jar for testing
    (test in Test) := {
      (Keys.`package` in Compile).value
        (test in Test).value
    },
    name := "squall-frontend",
    libraryDependencies += "org.apache.storm" % "storm-core" % "0.9.3" % "provided",
    libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
    libraryDependencies +=  "org.scalatest" % "scalatest_2.11" % "2.2.4" % Test,
    // Interactive mode
    // TODO: this should probably be a task
    scalacOptions in (Compile, console) += "-Yrepl-sync",
    scalacOptions in (Compile, console) += "-Yrepl-class-based",
    scalacOptions in (Compile, console) += "-Yrepl-outdir",
    scalacOptions in (Compile, console) += repl_classdir.getAbsolutePath(),
    initialCommands in Compile in console += "import ch.epfl.data.squall.query_plans.QueryBuilder;",
    initialCommands in Compile in console += "import ch.epfl.data.squall.api.scala.SquallType._;",
    initialCommands in Compile in console += "import ch.epfl.data.squall.api.scala.Stream._;",
    initialCommands in Compile in console += "import ch.epfl.data.squall.api.scala.TPCHSchema._;",
    initialCommands in Compile in console += s"""val REPL = new ch.epfl.data.squall.api.scala.REPL(\"${repl_outdir}\");""",
    initialCommands in Compile in console += "import REPL._;",
    initialCommands in Compile in console += "start;",
    cleanupCommands in Compile in console += "ch.epfl.data.squall.utilities.StormWrapper.shutdown();",
    cleanupCommands in Compile in console += "println(\"Shutting down...\");",
    console in Compile <<= (console in Compile).dependsOn(assembly)
  )


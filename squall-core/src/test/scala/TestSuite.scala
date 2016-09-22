/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.test

import ch.epfl.data.squall.main.Main
import ch.epfl.data.squall.storage.{BasicStore, KeyValueStore}
import ch.epfl.data.squall.utilities.{LocalMergeResults, SquallContext, StormWrapper, SystemParameters}
import java.io._

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.{Appender, Logger, LoggerContext}
import org.apache.logging.log4j.core.appender.FileAppender
import org.apache.logging.log4j.core.config.Configuration
import org.apache.logging.log4j.core.layout.PatternLayout
import org.scalatest._

class TestSuite extends FunSuite with BeforeAndAfterAll {

  abstract sealed class Mode {
    def getConf(confName: String): java.util.Map[_, _]
  }

  case class PlanRunner() extends Mode {
    def confDir = new File("../test/squall_plan_runner/confs/local")

    override def getConf(confName: String) = {
      val confPath = confDir + "/" + confName
      SystemParameters.fileToStormConfig(confPath)
    }
  }

  case class SQL() extends Mode {
    def confDir = new File("../test/squall/confs/local")

    override def getConf(confName: String) = {
      val parser = new ch.epfl.data.squall.api.sql.main.ParserMain()
      val confPath = confDir + "/" + confName
      SystemParameters.mapToStormConfig(parser.createConfig(confPath))
    }
  }

  object Logging {
    var fileAppender: FileAppender = _
    var log4j2Logger: Logger = _

    def beginLog(confName: String) = {
      val loggerContext: LoggerContext = LogManager.getContext(false).asInstanceOf[LoggerContext]

      log4j2Logger = loggerContext.getLogger(LogManager.ROOT_LOGGER_NAME)
      val configuration: Configuration = loggerContext.getConfiguration

      val verbosity = Option(System.getenv("SQUALL_LOG_VERBOSE")).getOrElse("FALSE")
      if (verbosity != "TRUE") {
        val appender: Appender = log4j2Logger.getAppenders.get("STDOUT")
        if (appender != null) {
          log4j2Logger.removeAppender(appender)
        }
      }

      val tempFile = File.createTempFile(confName, ".log")
      println("\tWriting test output to " + tempFile.getAbsolutePath)

      val layout: PatternLayout = PatternLayout
        .newBuilder()
        .withConfiguration(configuration)
        .withPattern("%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n")
        .build()

      fileAppender = FileAppender.createAppender(tempFile.getAbsolutePath, "false", "false",
        confName, "true", "true", "true", "8192", layout, null, "true", null,
        configuration)
      fileAppender.start()

      log4j2Logger.addAppender(fileAppender)

      loggerContext.updateLoggers()
    }

    def endLog() = {
      val loggerContext: LoggerContext = LogManager.getContext(false).asInstanceOf[LoggerContext]

      log4j2Logger.removeAppender(fileAppender)
      fileAppender.stop()
      loggerContext.updateLoggers()
    }
  }

  def runQuery(confName: String): BasicStore[Object] = {
    Logging.beginLog(confName)

    val conf = PlanRunner().getConf(confName)
    val queryPlan = Main.chooseQueryPlan(conf)

    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", confName)
    val context = new SquallContext(conf)

    val builder = queryPlan.createTopology(context)

    val result = StormWrapper.localSubmitAndWait(context, queryPlan)

    Logging.endLog()
    result
  }

  def runSQL(confName: String): BasicStore[Object] = {
    Logging.beginLog(confName)
    val parser = new ch.epfl.data.squall.api.sql.main.ParserMain()

    val conf = SQL().getConf(confName)

    val queryPlan = parser.generatePlan(conf)
    parser.putAckers(queryPlan, conf)
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", confName)
    val context = new SquallContext(conf)

    val builder = queryPlan.createTopology(context)
    val result = StormWrapper.localSubmitAndWait(context, queryPlan)

    Logging.endLog()
    result
  }

  def expectedResultFor(result: BasicStore[Object], confName: String, mode: Mode = PlanRunner()): BasicStore[Object] = {
    expectedResultFor(result, confName, x => x.toDouble.asInstanceOf[Object], mode)
  }

  def expectedResultFor[T](result: BasicStore[T], confName: String, convert: (String => T), mode: Mode): BasicStore[T] = {
    val conf = mode.getConf(confName)
    val resultsFile = LocalMergeResults.getResultFilePath(conf)

    val expected = new KeyValueStore[String,T](conf)

    val source = scala.io.Source.fromFile(resultsFile)
    source.getLines foreach { l =>
      val value: T = convert(l.split(" = ").last)

      val key = if (l.split(" = ").length > 1) {
        l.split(" = ")(0)
      } else {
        "SEK"
      }

      expected.insert( key, value.asInstanceOf[Object] )
    }

    expected
  }

  def testSQL(confName: String) = {
    test(confName) {
      val result = runSQL(confName)
      assert(result.equals(expectedResultFor(result, confName, mode = SQL())))
    }
  }

  // for (confFile <- confDir.listFiles()) {
  //   test(confFile.getName()) {
  //       ....
  //   }
  // }

  // test("") {
  //   val query = ""
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }


}


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

import org.scalatest._
import scala.sys.process._
import java.io._

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;

import ch.epfl.data.squall.utilities.StormWrapper
import ch.epfl.data.squall.utilities.SystemParameters
import ch.epfl.data.squall.storage.BasicStore
import ch.epfl.data.squall.storage.KeyValueStore
import ch.epfl.data.squall.storage.ValueStore
import ch.epfl.data.squall.types.SumCountType
import ch.epfl.data.squall.types.SumCount
import ch.epfl.data.squall.main.Main

// TODO: not usign LocalMergeResults
import ch.epfl.data.squall.utilities.LocalMergeResults

class PlanRunnerQueries extends FunSuite with BeforeAndAfterAll {
  val confDir = new File("../test/squall_plan_runner/confs/local")

  override def afterAll() {
    println("Shutting down local cluster")
    StormWrapper.shutdown()
  }

  def runQuery(confName: String): BasicStore[Object] = {
    // http://stackoverflow.com/questions/7824620/logback-set-log-file-name-programatically
    val loggerContext: LoggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]

    val fileAppender: FileAppender[ILoggingEvent] = new FileAppender()
    fileAppender.setContext(loggerContext)
    fileAppender.setName("timestamp")
    // set the file name
    val tempFile = File.createTempFile(confName, ".log")
    println("\tWriting test output to " + tempFile.getAbsolutePath())
    fileAppender.setFile(tempFile.getAbsolutePath())

    val encoder: PatternLayoutEncoder = new PatternLayoutEncoder()
    encoder.setContext(loggerContext);
    encoder.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
    encoder.start();

    fileAppender.setEncoder(encoder);
    fileAppender.start();

    // attach the rolling file appender to the logger of your choice
    val logbackLogger: Logger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    logbackLogger.addAppender(fileAppender)

    // OPTIONAL: print logback internal status messages
    //StatusPrinter.print(loggerContext)



    val confPath = confDir + "/" + confName
    val conf = SystemParameters.fileToStormConfig(confPath)
    val queryPlan = Main.chooseQueryPlan(conf)

    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", confName)

    val builder = queryPlan.createTopology(conf)

    val result = StormWrapper.localSubmitAndWait(conf, queryPlan)

    logbackLogger.detachAppender(fileAppender)
    fileAppender.stop();

    result
  }

  def expectedResultFor(result: BasicStore[Object], confName: String): BasicStore[Object] = {
    expectedResultFor(result, confName, x => x.toDouble.asInstanceOf[Object])
  }

  def expectedResultFor[T](result: BasicStore[T], confName: String,
                           convert: String => T): BasicStore[T] = {
    val confPath = confDir + "/" + confName
    val conf = SystemParameters.fileToStormConfig(confPath)
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

  test("0_01G_hyracks") {
    val query = "0_01G_hyracks"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_hyracks_l3_batch") {
    val query = "0_01G_hyracks_l3_batch"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_hyracks_pre_agg") {
    val query = "0_01G_hyracks_pre_agg"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }
// 0_01G_scalahyracks

  test("0_01G_theta_hyracks") {
    val query = "0_01G_theta_hyracks"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_input_dominated") {
    val query = "0_01G_theta_input_dominated"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_multiple_join") {
    val query = "0_01G_theta_multiple_join"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch10") {
    val query = "0_01G_theta_tpch10"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch3") {
    val query = "0_01G_theta_tpch3"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch4") {
    val query = "0_01G_theta_tpch4"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch5") {
    val query = "0_01G_theta_tpch5"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch7") {
    val query = "0_01G_theta_tpch7"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch8") {
    val query = "0_01G_theta_tpch8"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_theta_tpch9") {
    val query = "0_01G_theta_tpch9"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch10") {
    val query = "0_01G_tpch10"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch3") {
    val query = "0_01G_tpch3"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch4") {
    val query = "0_01G_tpch4"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch5") {
    val query = "0_01G_tpch5"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch5avg") {
    val query = "0_01G_tpch5avg"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query, (new SumCountType()).fromString(_))))
  }

  test("0_01G_tpch7") {
    val query = "0_01G_tpch7"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch8") {
    val query = "0_01G_tpch8"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("0_01G_tpch9") {
    val query = "0_01G_tpch9"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  test("10K_rst") {
    val query = "10K_rst"
    val result = runQuery(query)
    assert(result.equals(expectedResultFor(result, query)))
  }

  // test("0_01G_dbtoaster_hyracks") {
  //   val query = "0_01G_dbtoaster_hyracks"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_seqjoin_tpch3") {
  //   val query = "0_01G_dbtoaster_seqjoin_tpch3"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_seqjoin_tpch3_hash") {
  //   val query = "0_01G_dbtoaster_seqjoin_tpch3_hash"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_seqjoin_tpch5") {
  //   val query = "0_01G_dbtoaster_seqjoin_tpch5"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_seqjoin_tpch5_hash") {
  //   val query = "0_01G_dbtoaster_seqjoin_tpch5_hash"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_tpch17") {
  //   val query = "0_01G_dbtoaster_tpch17"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_tpch3") {
  //   val query = "0_01G_dbtoaster_tpch3"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_dbtoaster_tpch5") {
  //   val query = "0_01G_dbtoaster_tpch5"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

}


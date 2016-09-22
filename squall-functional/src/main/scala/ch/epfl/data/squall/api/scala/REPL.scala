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

package ch.epfl.data.squall.api.scala

import ch.epfl.data.squall.api.scala.SquallType._
import ch.epfl.data.squall.api.scala.Stream._
import ch.epfl.data.squall.api.scala.TPCHSchema._
import ch.epfl.data.squall.query_plans.QueryBuilder
import ch.epfl.data.squall.utilities.SquallContext
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.{Level, LogManager}
import scala.collection.JavaConversions._


/** This class does not actually offer a REPL, but instead provides some
  * useful setup and methods to be imported when starting a REPL, for instance
  * when using "sbt console"
  *
  * @param outdir a directory containing the classes generated during the REPL
  *               session. This should be the one set to "-Yrepl-outdir"
  *
  */
class REPL(val outdir: String) {
  System.setProperty("storm.options", "storm.meta.serialization.delegate=ch.epfl.data.squall.utilities.SquallSerializationDelegate," + s"squall.classdir=${outdir}/classes/")

  val context = new SquallContext()

  def start() = {
    print("""
 ____   ___  _   _   _    _     _
/ ___| / _ \| | | | / \  | |   | |
\___ \| | | | | | |/ _ \ | |   | |
 ___) | |_| | |_| / ___ \| |___| |___
|____/ \__\_\\___/_/   \_|_____|_____|

Type "help" for Squall related help

""")
    context.setLocal
    stopLogging()
  }

  // TODO: make a more useful help. Maybe use a Map to define the possible
  // commands and their documentation
  def help() = {
    println("You can use Squall API to build a query plan and submit it using \"submit(plan)\"")
  }

  private def packClasses(): String = {
    println("Packing jar file...")
    import scala.sys.process._
      (s"cp squall-functional/target/squall-frontend-standalone-0.2.0.jar ${outdir}/repl.jar").!!
      (s"jar uf ${outdir}/repl.jar -C ${outdir}/classes/ .").!!
      println("Done packing")
    s"${outdir}/repl.jar"
  }

  var count = 0
  def prepareSubmit(): String = {
    if (context.isDistributed()) {
      ////// Here comes the ugly part. We have to trick Storm, as we are doing
      ////// things that are not really standard.

      val jar = packClasses()

      // Now we have to trick storm into thinking we launched with the storm
      // script. This is easier!
      System.setProperty("storm.jar", jar);
      ////////////////////////
    }

    // Configure the query. To easily identify it we use the prefixes repl_0_,
    // repl_1_, repl_2_... Followed by a random number to avoid exceptions
    // telling us that the topology already exists.
    count = count + 1
    "repl_" + count + "_" + scala.util.Random.nextInt()
  }



  // If submitting locally we can actually get a result by calling submitLocal
  def submitLocal(plan: QueryBuilder) = {
    val tpname = prepareSubmit()
    context.submitLocal(tpname, plan)
  }

  def submitDistributed(plan: QueryBuilder) = {
    val tpname = prepareSubmit()
    context.submit(tpname, plan)
  }

  private val loggerContext: LoggerContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
  def activateLogging() = {
    val loggerConfig = loggerContext.getConfiguration().getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    loggerConfig.setLevel(Level.INFO)
    loggerContext.updateLoggers()
  }

  def stopLogging() = {
    val loggerConfig = loggerContext.getConfiguration().getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    loggerConfig.setLevel(Level.OFF)
    loggerContext.updateLoggers()
  }



  // An example query plan
  def createQueryPlan(): QueryBuilder = {
    val customers = Source[Customer]("customer").map( c => (c.custkey, c.mktsegment) )
    val orders = Source[Orders]("orders").map( _.custkey )
    val join = (customers join orders)( _._1 )( x => x )
    val agg = join.groupByKey( x => 1, _._1._2)
    val plan = agg.execute(context)
    plan
  }

}

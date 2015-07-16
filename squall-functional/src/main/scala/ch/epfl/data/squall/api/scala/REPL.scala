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

import ch.epfl.data.squall.query_plans.QueryBuilder
import ch.epfl.data.squall.api.scala.SquallType._
import ch.epfl.data.squall.api.scala.Stream._
import ch.epfl.data.squall.api.scala.TPCHSchema._

import scala.collection.JavaConversions._

import ch.epfl.data.squall.utilities.StormWrapper
import ch.epfl.data.squall.utilities.SystemParameters

import backtype.storm.Config

/** This class does not actually offer a REPL, but instead provides some
  * useful setup and methods to be imported when starting a REPL, for instance
  * when using "sbt console"
  *
  * @param outdir a directory containing the classes generated during the REPL
  *               session. This should be the one set to "-Yrepl-outdir"
  *
  */
class REPL(val outdir: String) {
  // TODO: eventually most of this should be handled by a "context object"
  // Initialize
  val conf = new Config
  var local = false

  // Load default values
  SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME_PREFIX", "username")
  SystemParameters.putInMap(conf, "DIP_EXTENSION", ".tbl")
  SystemParameters.putInMap(conf, "DIP_READ_SPLIT_DELIMITER", "\\|")
  SystemParameters.putInMap(conf, "DIP_GLOBAL_ADD_DELIMITER", "|")
  SystemParameters.putInMap(conf, "DIP_GLOBAL_SPLIT_DELIMITER", "\\|")

  SystemParameters.putInMap(conf, "DIP_KILL_AT_THE_END", "true")

  SystemParameters.putInMap(conf, "STORAGE_LOCAL_DIR", "/tmp/ramdisk")
  SystemParameters.putInMap(conf, "STORAGE_CLUSTER_DIR", "/data/squall_zone/storage")
  SystemParameters.putInMap(conf, "STORAGE_COLD_START", "true")
  SystemParameters.putInMap(conf, "STORAGE_MEMORY_SIZE_MB", "4096")

  // Configure for tpch
  SystemParameters.putInMap(conf, "CUSTOMER_PAR", "1")
  SystemParameters.putInMap(conf, "ORDERS_PAR", "1")
  SystemParameters.putInMap(conf, "LINEITEM_PAR", "1")
  SystemParameters.putInMap(conf, "CUSTOMER_ORDERS_PAR", "1")
  SystemParameters.putInMap(conf, "CUSTOMER_ORDERS_LINEITEM_PAR", "1")

  def start() = {
print("""
 ____   ___  _   _   _    _     _
/ ___| / _ \| | | | / \  | |   | |
\___ \| | | | | | |/ _ \ | |   | |
 ___) | |_| | |_| / ___ \| |___| |___
|____/ \__\_\\___/_/   \_|_____|_____|

Type "help" for Squall related help

""")
    setLocal
  }

  // TODO: make a more useful help. Maybe use a Map to define the possible
  // commands and their documentation
  def help() = {
    println("You can use Squall API to build a query plan and submit it using \"submit(plan)\"")
  }

  private def packClasses(): String = {
    println("Packing jar file...")
    import scala.sys.process._
    if(local) {
      (s"jar cf ${outdir}/repl.jar -C ${outdir}/classes/ .").!!
    } else {
      (s"cp squall-functional/target/squall-frontend-standalone-0.2.0.jar ${outdir}/repl.jar").!!
      (s"jar uf ${outdir}/repl.jar -C ${outdir}/classes/ .").!!
    }
    println("Done packing")
    s"${outdir}/repl.jar"
  }

  var count = 0
  def prepareSubmit(): String = {
    val jar = packClasses()

    ////// Here comes the ugly part. We have to trick Storm, as we are doing
    ////// things that are not really standard.

    //// TODO: HACK FOR STORM 0.9.3, if we ever go to 0.9.4 this won't be necessary (I think)
    // In storm 0.9.3 once one jar is submitted, no other jar can be submitted
    // as it assumes that it has already been submitted.
    // We can use Java reflection to hack into StormSubmitter and "reset" it,
    // so we can use submit multiple topologies during one run.
    import backtype.storm.StormSubmitter
    import java.lang.reflect.Field
    val f : Field = (new StormSubmitter()).getClass().getDeclaredField("submittedJar");
    f.setAccessible(true);
    f.set(new StormSubmitter(), null);

    // Now we have to trick storm into thinking we launched with the storm
    // script. This is easier!
    System.setProperty("storm.jar", jar);
    ////////////////////////

    // Configure the query. To easily identify it we use the prefixes repl_0_,
    // repl_1_, repl_2_... Followed by a random number to avoid exceptions
    // telling us that the topology already exists.
    val tpname = "repl_" + count + "_" + scala.util.Random.nextInt()
    SystemParameters.putInMap(conf, "DIP_QUERY_NAME", "repl_" + count)
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", tpname)
    count = count + 1

    tpname
  }

  def submit(queryPlan: QueryBuilder) = {
    val tpname = prepareSubmit()

    // Create and send the topology
    if (local) {
      StormWrapper.localSubmitAndWait(conf, queryPlan)
    } else {
      StormWrapper.submitTopology(conf, queryPlan.createTopology(conf))
      println("Submitted topology as " + tpname)
    }
  }

  def setDistributed() = {
    SystemParameters.putInMap(conf, "DIP_DISTRIBUTED", "true")
    SystemParameters.putInMap(conf, "DIP_DATA_PATH", "/shared/tpch/0.01G/")

    local = false
    println("Mode set to local")
  }

  def setLocal() = {
    SystemParameters.putInMap(conf, "DIP_DATA_PATH", "test/data/tpch/0.01G/")
    SystemParameters.putInMap(conf, "DIP_DISTRIBUTED", "false")
    SystemParameters.putInMap(conf, "DIP_NUM_ACKERS", 0)

    local = true
    println("Mode set to local")
  }

  // An example query plan
  def createQueryPlan(): QueryBuilder = {
    val customers = Source[Customer]("customer").map( c => (c.custkey, c.mktsegment) )
    val orders = Source[Orders]("orders").map( _.custkey )
    val join = (customers join orders)( _._1 )( x => x )
    val agg = join.groupByKey( x => 1, _._1._2)
    val plan = agg.execute(conf)
    plan
  }

}

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

class PlanRunnerQueries extends FunSuite {
  val confDir = new File("../test/squall_plan_runner/confs/local")

  def runQuery(conf: String): Int = {
    val log = File.createTempFile("squall_", ".log"); // prefix and suffix
    val out = (Process(s"./squall_local.sh PLAN_RUNNER $conf", new File("../bin")) #> log)!

    if (out != 0)
      println("Error: test '" + conf.split('/').last + "' failed. Error log in " + log.getAbsolutePath());

    out
  }

  for (confFile <- confDir.listFiles()) {
    test(confFile.getName()) {
      assertResult(0) {
        runQuery(confFile.getAbsolutePath())
      }
    }
  }

}


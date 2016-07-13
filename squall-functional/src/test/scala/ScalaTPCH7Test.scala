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

import ch.epfl.data.squall.utilities.SquallContext
import ch.epfl.data.squall.api.scala.queries.ScalaTPCH7Plan
import org.scalatest.FunSuite


class ScalaTPCH7Test extends FunSuite {

  val context = new SquallContext();

  test("0_01_scalatpch7") {
    expect(List()) {
      //Logging.beginLog("scalatpch7")
      context.setLocal()
      val plan = ScalaTPCH7Plan.getQueryPlan(context)
      val result = context.submitLocalAndWait("scalatpch7", plan)
      //Logging.endLog()
      List()
    }
  }


  // test("0_01G_hyracks") {
  //   val query = "0_01G_hyracks"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_hyracks_l3_batch") {
  //   val query = "0_01G_hyracks_l3_batch"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }

  // test("0_01G_hyracks_pre_agg") {
  //   val query = "0_01G_hyracks_pre_agg"
  //   val result = runQuery(query)
  //   assert(result.equals(expectedResultFor(result, query)))
  // }
  // 0_01G_scalahyracks

}
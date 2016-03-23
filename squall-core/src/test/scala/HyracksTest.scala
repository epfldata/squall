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

import java.util.ArrayList

import ch.epfl.data.squall.utilities.SquallContext
import ch.epfl.data.squall.examples.imperative.shj.HyracksPlan

class HyracksTest extends TestSuite {

  val context = new SquallContext();

  test("0_01G_hyracks") {
    expect(List(3706, 3007, 2536, 2772, 2979)) {
      Logging.beginLog("hyracks")
      context.setLocal()
      val plan = new HyracksPlan("test/data/tpch", ".tbl", context.getConfiguration()).getQueryPlan()
      val result = context.submitLocalAndWait("hyracks", plan)
      Logging.endLog()
      List("BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD", "AUTOMOBILE") map
      { result.access(_).get(0) }
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

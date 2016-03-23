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

class ThetaTest extends TestSuite {
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

}

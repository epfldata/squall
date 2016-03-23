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

import ch.epfl.data.squall.types.SumCountType

class TPCHTest extends TestSuite {
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
    assert(result.equals(expectedResultFor(result, query, (new SumCountType()).fromString(_), PlanRunner())))
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
}

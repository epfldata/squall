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

package ch.epfl.data.squall.test.sql

import ch.epfl.data.squall.test.TestSuite

class SqlHyracksTest extends TestSuite {

  testSQL("0_01G_distinct_hyracks_ncl")
  testSQL("0_01G_hyracks_irb")
  testSQL("0_01G_hyracks_is")
  testSQL("0_01G_hyracks_ncl")
  testSQL("0_01G_hyracks_nmcl")
  testSQL("0_01G_hyracks_nmpl")
  testSQL("0_01G_hyracks_nrl")


}

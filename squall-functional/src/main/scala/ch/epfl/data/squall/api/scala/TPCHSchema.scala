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

/**
 * @author mohamed
 */

object TPCHSchema {

  type customer = Tuple8[Int, String, String, Int, String, Double, String, String]
  type orders = Tuple9[Int, Int, String, Double, java.util.Date, String, String, Int, String]
  type lineitems = Tuple16[Int, Int, Int, Int, Double, Double, Double, Double, String, String, java.util.Date, java.util.Date, java.util.Date, String, String, String]
  type region = Tuple3[Int, String, String]
  type nation = Tuple4[Int, String, Int, String]
  type partsupp = Tuple5[Int, Int, Int, Double, String]
  type supplier = Tuple7[Int, String, String, Int, String, Double, String]
  type part = Tuple9[Int, String, String, String, String, Int, String, Double, String]

}
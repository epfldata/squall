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

  case class Customer(
    custkey    : Int,
    name       : String,
    address    : String,
    nationkey  : Int,
    phone      : String,
    acctbal    : Double,
    mktsegment : String,
    comment    : String
  )
  case class Orders(
    orderkey      : Int,
    custkey       : Int,
    orderstatus   : String,
    totalprice    : Double,
    orderdate     : java.util.Date,
    orderpriority : String,
    clerk         : String,
    shippriority  : Int,
    comment       : String
  )
  case class Lineitems(
    orderkey      : Int,
    partkey       : Int,
    suppkey       : Int,
    linenumber    : Int,
    quantity      : Double,
    extendedprice : Double,
    discount      : Double,
    tax           : Double,
    returnflag    : String,
    linestatus    : String,
    shipdate      : java.util.Date,
    commitdate    : java.util.Date,
    receiptdate   : java.util.Date,
    shipinstruct  : String,
    shipmode      : String,
    comment       : String
  )
  case class Region(
    regionkey : Int,
    name      : String,
    comment   : String
  )
  case class Nation(
    nationkey : Int,
    name      : String,
    regionkey : Int,
    comment   : String
  )
  case class Partsupp(
    partkey    : Int,
    suppkey    : Int,
    availqty   : Int,
    supplycost : Double,
    comment    : String
  )
  case class Supplier(
    suppkey   : Int,
    name      : String,
    address   : String,
    nationkey : Int,
    phone     : String,
    acctbal   : Double,
    comment   : String
  )
  case class Part(
    partkey     : Int,
    name        : String,
    mfgr        : String,
    brand       : String,
    parttype    : String,
    size        : Int,
    container   : String,
    retailprice : Double,
    comment     : String
  )
}

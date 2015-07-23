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

package ch.epfl.data.squall.api.scala.operators

import ch.epfl.data.squall.operators.AggregateOperator
import ch.epfl.data.squall.visitors.OperatorVisitor
import ch.epfl.data.squall.operators.AbstractOperator
import ch.epfl.data.squall.operators.DistinctOperator
import ch.epfl.data.squall.expressions.ValueExpression
import ch.epfl.data.squall.operators.ProjectOperator
import ch.epfl.data.squall.storage.BasicStore
import scala.reflect.runtime.universe._
import scala.math.Numeric
import org.apache.log4j.Logger
import java.util.Arrays.ArrayList
import ch.epfl.data.squall.storage.AggregationStorage
import ch.epfl.data.squall.api.scala.SquallType._
import java.util.Arrays
import org.apache.commons.lang.ArrayUtils
import Numeric.Implicits._
import scala.collection.JavaConverters._
import ch.epfl.data.squall.utilities.MyUtilities
import java.util.Arrays.ArrayList
import java.util.ArrayList
import java.util.Arrays.ArrayList
import ch.epfl.data.squall.storage._
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager
import ch.epfl.data.squall.types.Type;

class ScalaAggregateOperator[T: SquallType, A: Numeric](val _agg: T => A, val _map: java.util.Map[_, _]) extends AggregateOperator[A] {

  private val serialVersionUID = 1L
  //private val log = Logger.getLogger(getClass.getName)

  // the GroupBy type
  val GB_UNSET = -1
  val GB_COLUMNS = 0
  val GB_PROJECTION = 1

  var _distinct: DistinctOperator = null
  var _groupByType = GB_UNSET
  var _groupByColumns = new java.util.ArrayList[Integer]()
  var _groupByProjection: ProjectOperator = null
  var _numTuplesProcessed = 0
  var _storage: BasicStore[A] = new ScalaAggregationStorage[A](this, _map, true)

  var _windowRangeSecs = -1
  var _slideRangeSecs = -1

  override def accept(ov: OperatorVisitor): Unit = {
    ov.visit(this)
  }

  def getNewInstance(): ScalaAggregateOperator[T, A] = {

    val clone = new ScalaAggregateOperator(_agg, _map)
    if (_windowRangeSecs > 0 || _slideRangeSecs > 0)
      clone.SetWindowSemantics(_windowRangeSecs, _slideRangeSecs)
    clone
  }

  def alreadySetOther(GB_COLUMNS: Int): Boolean = {
    return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
  }

  override def clearStorage(): Unit = {
    _storage.reset()
  }

  override def getContent(): java.util.List[String] = {
    throw new UnsupportedOperationException(
      "getContent for ScalaAggregateOperator is not supported yet.");
  }

  override def getDistinct(): DistinctOperator = {
    _distinct
  }

  override def getExpressions(): java.util.List[ValueExpression[_]] = {
    throw new UnsupportedOperationException(
      "getExpressions for ScalaAggregateOperator is not supported yet.");
  }

  override def getGroupByColumns(): java.util.List[Integer] = {
    _groupByColumns;
  }

  override def getGroupByProjection(): ProjectOperator = {
    _groupByProjection
  }

  def getGroupByStr(): String = {
    var sb: StringBuilder = new StringBuilder();
    sb.append("(");
    for (i <- 0 until _groupByColumns.size()) {
      sb.append(_groupByColumns.get(i));
      if (i == _groupByColumns.size() - 1)
        sb.append(")");
      else
        sb.append(", ");
    }
    return sb.toString();
  }

  override def getNumTuplesProcessed(): Int = {
    _numTuplesProcessed
  }

  override def getStorage(): BasicStore[A] = {
    _storage.asInstanceOf[BasicStore[A]]
  }

  override def getType(): Type[Number] = {
    null
  }

  override def hasGroupBy(): Boolean = {
    _groupByType != GB_UNSET;
  }

  override def isBlocking(): Boolean = {
    return true;
  }

  override def printContent(): String = {
    _storage.getContent();
  }

  override def process(tupleList: java.util.List[String], lineageTimestamp: Long): java.util.List[java.util.List[String]] = {
    _numTuplesProcessed += 1;
    val refinedTuple =
      if (_distinct != null) {
        val refinedTuple = _distinct.processOne(tupleList, lineageTimestamp);
        if (refinedTuple == null)
          return null;
        refinedTuple
      } else tupleList

    val tupleHash = if (_groupByType == GB_PROJECTION)
      MyUtilities.createHashString(refinedTuple, _groupByColumns,
        _groupByProjection.getExpressions(), _map)
    else
      MyUtilities.createHashString(refinedTuple, _groupByColumns, _map)

    val value: A = _storage.update(refinedTuple, tupleHash, Long.box(lineageTimestamp));
    val strValue: String = value.toString();

    // propagate further the affected tupleHash-tupleValue pair
    val affectedTuple: java.util.List[String] = new ArrayList[String]();
    affectedTuple.add(tupleHash);
    affectedTuple.add(strValue);

    val result = new ArrayList[java.util.List[String]]()
    result.add(affectedTuple)
    return result;

  }

  override def runAggregateFunction(x$1: A, x$2: A): A = {
    x$1 + x$2
  }

  override def runAggregateFunction(x$1: A, x$2: java.util.List[String]): A = {
    val squalType: SquallType[T] = implicitly[SquallType[T]]
    val scalaList = x$2.asScala.toList
    val squallTuple = squalType.convertBack(scalaList)
    val res = _agg(squallTuple)
    x$1 + res
  }

  override def setDistinct(distinct: DistinctOperator): ScalaAggregateOperator[T, A] = {
    _distinct = distinct;
    return this;
  }

  // from AgregateOperator

  override def setGroupByColumns(groupByColumns: java.util.List[Integer]): ScalaAggregateOperator[T, A] = {
    if (groupByColumns == null)
      return this
    if (!alreadySetOther(GB_COLUMNS)) {
      _groupByType = GB_COLUMNS;
      _groupByColumns = new ArrayList(groupByColumns)
      _storage.setSingleEntry(false);
      return this;
    } else
      throw new RuntimeException("Aggragation already has groupBy set!");
  }

  override def setGroupByColumns(hashIndexes: Int*): ScalaAggregateOperator[T, A] = {
    setGroupByColumns(Arrays.asList(ArrayUtils.toObject(hashIndexes.toArray)).asInstanceOf[java.util.ArrayList[Integer]]);
  }

  override def setGroupByProjection(groupByProjection: ProjectOperator): ScalaAggregateOperator[T, A] = {
    if (!alreadySetOther(GB_PROJECTION)) {
      _groupByType = GB_PROJECTION;
      _groupByProjection = groupByProjection;
      _storage.setSingleEntry(false);
      return this;
    } else
      throw new RuntimeException("Aggragation already has groupBy set!");
  }

  override def toString(): String = {
    var sb: StringBuilder = new StringBuilder();
    sb.append("AggregateSumOperator with VE: ");

    if (_groupByColumns.isEmpty() && _groupByProjection == null)
      sb.append("\n  No groupBy!");
    else if (!_groupByColumns.isEmpty())
      sb.append("\n  GroupByColumns are ").append(getGroupByStr()).append(".");
    else if (_groupByProjection != null)
      sb.append("\n  GroupByProjection is ").append(_groupByProjection.toString())
        .append(".");
    if (_distinct != null)
      sb.append("\n  It also has distinct ").append(_distinct.toString());
    return sb.toString();
  }

  def SetWindowSemantics(windowRangeInSeconds: Int, windowSlideInSeconds: Int): AggregateOperator[A] = {
    WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
    _windowRangeSecs = windowRangeInSeconds;
    _slideRangeSecs = windowSlideInSeconds;
    _storage = new ScalaWindowAggregationStorage[A](this, _map, true, _windowRangeSecs, _slideRangeSecs);
    if (_groupByColumns != null || _groupByProjection != null)
      _storage.setSingleEntry(false);
    return this;
  }

  @Override
  def SetWindowSemantics(windowRangeInSeconds: Int): AggregateOperator[A] = {
    SetWindowSemantics(windowRangeInSeconds, windowRangeInSeconds);
  }

  def getWindowSemanticsInfo(): Array[Int] = {
    val res = Array(_windowRangeSecs, _slideRangeSecs);
    return res;
  }

}

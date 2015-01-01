package frontend.functional.scala.operators

import plan_runner.operators.AggregateOperator
import plan_runner.visitors.OperatorVisitor
import plan_runner.operators.DistinctOperator
import plan_runner.expressions.ValueExpression
import plan_runner.operators.ProjectOperator
import plan_runner.storage.BasicStore
import plan_runner.conversion.TypeConversion
import scala.reflect.runtime.universe._
import scala.math.Numeric
import org.apache.log4j.Logger
import java.util.Arrays.ArrayList
import plan_runner.conversion.NumericConversion
import plan_runner.storage.AggregationStorage
import frontend.functional.scala.Types.SquallType
import java.util.Arrays
import org.apache.commons.lang.ArrayUtils
import Numeric.Implicits._
import scala.collection.JavaConverters._
import plan_runner.utilities.MyUtilities
import java.util.Arrays.ArrayList
import java.util.ArrayList

class ScalaAggregateOperator[T:SquallType, A:Numeric](_agg: T => A, _keyIndices: List[Int], _map:java.util.Map[_,_]) extends AggregateOperator[A] {
  
  
  private val serialVersionUID = 1L;
  private val log = Logger.getLogger(getClass.getName);

  // the GroupBy type
  val GB_UNSET = -1;
  val GB_COLUMNS = 0;
  val GB_PROJECTION = 1;

  var _distinct:DistinctOperator
  var _groupByType = GB_UNSET;
  var _groupByColumns = new java.util.ArrayList[Integer]();
  var _groupByProjection:ProjectOperator;
  var _numTuplesProcessed = 0;
  val _storage:AggregationStorage[A]= new ScalaAggregationStorage[A](this, _map, true);

  
  override def accept(ov: OperatorVisitor): Unit = {
    ov.visit(this)
  }
  
  def alreadySetOther(GB_COLUMNS:Int):Boolean = {
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
  
   def getGroupByStr():String = {
    var sb:StringBuilder = new StringBuilder();
    sb.append("(");
    for (i <- 0 to _groupByColumns.size()) {
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

  override def getType(): TypeConversion[Number] = {
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

  override def process(tupleList: java.util.List[String]): java.util.List[String] = {
    
    _numTuplesProcessed+=1;
    
    val refinedTuple=
    if (_distinct != null) {
      val refinedTuple = _distinct.process(tupleList);
      if (refinedTuple == null)
        return null;
      refinedTuple
    }
    else null
    
    val tupleHash = if (_groupByType == GB_PROJECTION)
      MyUtilities.createHashString(refinedTuple, _groupByColumns,
          _groupByProjection.getExpressions(), _map)
    else
      MyUtilities.createHashString(refinedTuple, _groupByColumns, _map)
      
    val value:A = _storage.update(refinedTuple, tupleHash);
    val strValue:String = value.toString();
    
    // propagate further the affected tupleHash-tupleValue pair
    val affectedTuple:java.util.List[String] = new ArrayList[String]();
    affectedTuple.add(tupleHash);
    affectedTuple.add(strValue);

    return affectedTuple;
    
  }

  override def runAggregateFunction(x$1: A, x$2: A): A = {
    x$1+x$2
  }

  override def runAggregateFunction(x$1: A, x$2: java.util.List[String]): A = {
    val squalType: SquallType[T] = implicitly[SquallType[T]]
    val scalaList= x$2.asScala.toList
    val squallTuple= squalType.convertBack(scalaList)
    val res=_agg(squallTuple)
    x$1+res
  }
  
  @Override
  override def setDistinct(distinct:DistinctOperator):ScalaAggregateOperator[T,A] = {
    _distinct = distinct;
    return this;
  }

  // from AgregateOperator
  @Override
   override def setGroupByColumns(groupByColumns:java.util.List[Integer]):ScalaAggregateOperator[T,A] = {
    if (!alreadySetOther(GB_COLUMNS)) {
      _groupByType = GB_COLUMNS;
      _groupByColumns = groupByColumns.asInstanceOf[java.util.ArrayList[Integer]];
      _storage.setSingleEntry(false);
      return this;
    } else
      throw new RuntimeException("Aggragation already has groupBy set!");
  }
  
  @Override
  override def setGroupByColumns(hashIndexes:Int*):ScalaAggregateOperator[T,A] ={
    setGroupByColumns(Arrays.asList(ArrayUtils.toObject(hashIndexes.toArray)).asInstanceOf[java.util.ArrayList[Integer]]);
  }

  @Override
  override def setGroupByProjection(groupByProjection:ProjectOperator):ScalaAggregateOperator[T,A]= {
    if (!alreadySetOther(GB_PROJECTION)) {
      _groupByType = GB_PROJECTION;
      _groupByProjection = groupByProjection;
      _storage.setSingleEntry(false);
      return this;
    } else
      throw new RuntimeException("Aggragation already has groupBy set!");
  }

  @Override
  override def toString():String= {
    var sb:StringBuilder = new StringBuilder();
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

  
}
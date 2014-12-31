package frontend.functional.scala.operators

import plan_runner.operators.AggregateOperator
import plan_runner.visitors.OperatorVisitor
import plan_runner.operators.DistinctOperator
import plan_runner.expressions.ValueExpression
import plan_runner.operators.ProjectOperator
import plan_runner.storage.BasicStore
import plan_runner.conversion.TypeConversion
import scala.reflect.runtime.universe._

abstract class ScalaAggregateOperator[Number] extends AggregateOperator[Number] {
  def accept(ov: OperatorVisitor): Unit = {
    ???
  }

  def clearStorage(): Unit = {
    ???
  }

  def getContent(): java.util.List[String] = {
    ???
  }

  def getDistinct(): DistinctOperator = {
    ???
  }

  def getExpressions(): java.util.List[ValueExpression[Number]] = {
    ???
  }

  def getGroupByColumns(): java.util.List[Integer] = {
    ???
  }

  def getGroupByProjection(): ProjectOperator = {
    ???
  }

  def getNumTuplesProcessed(): Int = {
    ???
  }

  def getStorage(): BasicStore[Number] = {
    ???
  }

  def getType(): TypeConversion[Number] = {
    ???
  }

  def hasGroupBy(): Boolean = {
    ???
  }

  def isBlocking(): Boolean = {
    ???
  }

  def printContent(): String = {
    ???
  }

  def process(tuple: java.util.List[String]): java.util.List[String] = {
    ???
  }
}
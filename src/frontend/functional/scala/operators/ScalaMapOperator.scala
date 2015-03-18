package frontend.functional.scala.operators

import ch.epfl.data.plan_runner.operators.Operator
import ch.epfl.data.plan_runner.visitors.OperatorVisitor
import frontend.functional.scala.Types.SquallType
import scala.collection.JavaConverters._

/**
 * @author mohamed
 */
class ScalaMapOperator[T: SquallType, U: SquallType](fn: T => U) extends Operator {

  private var _numTuplesProcessed: Int = 0;

  def accept(ov: OperatorVisitor): Unit = {
    //ov.visit(this);
  }

  def getContent(): java.util.List[String] = {
    throw new RuntimeException("getContent for SelectionOperator should never be invoked!")
  }

  def getNumTuplesProcessed(): Int = {
    _numTuplesProcessed
  }

  def isBlocking(): Boolean = {
    false
  }

  def printContent(): String = {
    throw new RuntimeException("printContent for SelectionOperator should never be invoked!");
  }

  def process(tuple: java.util.List[String], lineageTimestamp:Long): java.util.List[String] = {
    _numTuplesProcessed += 1;
    val squalTypeInput: SquallType[T] = implicitly[SquallType[T]]
    val squalTypeOutput: SquallType[U] = implicitly[SquallType[U]]
    val scalaList = tuple.asScala.toList
    val squallTuple = squalTypeInput.convertBack(scalaList)
    val cmp = fn(squallTuple)
    val res = squalTypeOutput.convert(cmp)
    seqAsJavaListConverter(res).asJava
  }
}
package frontend.functional.scala.operators

import ch.epfl.data.plan_runner.storage.AggregationStorage
import ch.epfl.data.plan_runner.operators.AggregateOperator
import ch.epfl.data.plan_runner.conversion.TypeConversion
import Numeric.Implicits._
import ch.epfl.data.plan_runner.storage.WindowAggregationStorage

/**
 * @author mohamed
 */
class ScalaWindowAggregationStorage[A: Numeric](outerAggOp: AggregateOperator[A], map: java.util.Map[_, _],
                                                singleEntry: Boolean, windowedRange: Int, slidelength: Int) extends WindowAggregationStorage[A](outerAggOp, null, map, singleEntry, windowedRange, slidelength) {

  @Override
  override def getInitialValue(): A = {
    val init: A = implicitly[Numeric[A]].zero
    init
  }

}
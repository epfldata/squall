package ch.epfl.data.squall.api.scala.operators

import ch.epfl.data.squall.operators.AggregateOperator
import ch.epfl.data.squall.storage.WindowAggregationStorage

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
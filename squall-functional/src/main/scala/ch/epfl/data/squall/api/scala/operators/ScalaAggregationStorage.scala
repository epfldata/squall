package ch.epfl.data.squall.api.scala.operators

import ch.epfl.data.squall.storage.AggregationStorage
import ch.epfl.data.squall.operators.AggregateOperator
import ch.epfl.data.squall.conversion.TypeConversion
import Numeric.Implicits._

/**
 * @author mohamed
 */
class ScalaAggregationStorage[A: Numeric](outerAggOp: AggregateOperator[A], map: java.util.Map[_, _],
                                          singleEntry: Boolean) extends AggregationStorage[A](outerAggOp, null, map, singleEntry) {

  @Override
  override def getInitialValue(): A = {
    val init: A = implicitly[Numeric[A]].zero
    init
  }

}
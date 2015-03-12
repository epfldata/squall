package frontend.functional.scala.operators

import ch.epfl.data.plan_runner.storage.AggregationStorage
import ch.epfl.data.plan_runner.operators.AggregateOperator
import ch.epfl.data.plan_runner.conversion.TypeConversion
import Numeric.Implicits._


/**
 * @author mohamed
 */
class ScalaAggregationStorage[A:Numeric](outerAggOp:AggregateOperator[A], map:java.util.Map[_,_],
      singleEntry:Boolean) extends AggregationStorage[A](outerAggOp, null, map, singleEntry) {
  
      @Override
      override def getInitialValue():A={
        val init: A = implicitly[Numeric[A]].zero
        init
      }
  
}
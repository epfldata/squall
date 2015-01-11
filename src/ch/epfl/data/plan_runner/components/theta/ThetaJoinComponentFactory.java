package ch.epfl.data.plan_runner.components.theta;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class ThetaJoinComponentFactory {

	public static Component createThetaJoinOperator(int thetaJoinType, Component firstParent,
			Component secondParent, QueryBuilder queryBuilder) {
		Component result = null;
		if (thetaJoinType == SystemParameters.STATIC_CIS){
			result = new ThetaJoinStaticComponent(firstParent, secondParent, false);
		}else if (thetaJoinType == SystemParameters.EPOCHS_CIS){
			result = new ThetaJoinDynamicComponentAdvisedEpochs(firstParent, secondParent);
		}else if (thetaJoinType == SystemParameters.STATIC_CS){
			result = new ThetaJoinStaticComponent(firstParent, secondParent, true);
		}else if (thetaJoinType == SystemParameters.EPOCHS_CS){
			result = new ThetaJoinDynamicComponentAdvisedEpochs(firstParent, secondParent);
		}else{
			throw new RuntimeException("Unsupported Thtea Join Type");
		}
		queryBuilder.add(result);
		return result;
	}
}
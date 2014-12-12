package plan_runner.components.theta;

import plan_runner.components.Component;
import plan_runner.query_plans.QueryPlan;
import plan_runner.utilities.SystemParameters;

public class ThetaJoinComponentFactory {

	public static Component createThetaJoinOperator(int thetaJoinType, Component firstParent,
			Component secondParent, QueryPlan queryPlan) {
		
		if (thetaJoinType == SystemParameters.STATIC_CIS){
			return new ThetaJoinStaticComponent(firstParent, secondParent, queryPlan, false);
		}

		else if (thetaJoinType == SystemParameters.EPOCHS_CIS)
			return new ThetaJoinDynamicComponentAdvisedEpochs(firstParent, secondParent, queryPlan);

		else if (thetaJoinType == SystemParameters.STATIC_CS)
			return new ThetaJoinStaticComponent(firstParent, secondParent, queryPlan, true);

		else if (thetaJoinType == SystemParameters.EPOCHS_CS)
			return new ThetaJoinDynamicComponentAdvisedEpochs(firstParent, secondParent, queryPlan);

		else
			throw new RuntimeException("Unsupported Thtea Join Type");
	}
}

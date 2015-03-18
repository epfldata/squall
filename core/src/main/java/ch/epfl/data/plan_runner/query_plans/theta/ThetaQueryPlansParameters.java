package ch.epfl.data.plan_runner.query_plans.theta;

import java.util.Map;

public class ThetaQueryPlansParameters {

	/**
	 * (ThetaJoinType = 0 = STATIC PARTITIONING) (ThetaJoinType = 1 = DYNAMIC
	 * PARTITIONING ADVISED NON-BLOCKING EPOCHS) (ThetaJoinType = 2 = STATIC
	 * PARTITIONING MBUCKET) (ThetaJoinType = 3 = DYNAMIC PARTITIONING ADVISED
	 * NON-BLOCKING EPOCHS MBUCKET)
	 */

	public static long getThetaDynamicRefreshRate(Map conf) {
		final String refreshRate = (String) conf
				.get("DIP_THETA_CLOCK_REFRESH_RATE_MILLISECONDS");
		return Long.valueOf(refreshRate);
	}

	public static int getThetaJoinType(Map conf) {
		final String type = (String) conf.get("DIP_JOIN_TYPE");
		return Integer.valueOf(type);

	}

}

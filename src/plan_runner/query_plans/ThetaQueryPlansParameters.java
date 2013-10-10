package plan_runner.query_plans;

import java.util.Map;

public class ThetaQueryPlansParameters {

	/**
	 * (ThetaJoinType = 0 = STATIC PARTITIONING) (ThetaJoinType = 3 = DYNAMIC
	 * PARTITIONING ADVISED NON-BLOCKING EPOCHS)
	 */

	public static long getThetaDynamicRefreshRate(Map conf) {
		final String refreshRate = (String) conf.get("DIP_THETA_CLOCK_REFRESH_RATE_MILLISECONDS");
		return Long.valueOf(refreshRate);
	}

	public static int getThetaJoinType(Map conf) {
		final String type = (String) conf.get("DIP_JOIN_TYPE");
		return Integer.valueOf(type);

	}

}

/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.query_plans;

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

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


package ch.epfl.data.squall.components.theta;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.JoinerComponent;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.SystemParameters;

public class ThetaJoinComponentFactory {

	public static JoinerComponent createThetaJoinOperator(int thetaJoinType,
			Component firstParent, Component secondParent,
			QueryBuilder queryBuilder) {
		JoinerComponent result = null;
		if (thetaJoinType == SystemParameters.STATIC_CIS) {
			result = new ThetaJoinComponent(firstParent, secondParent,
					false);
		} else if (thetaJoinType == SystemParameters.EPOCHS_CIS) {
			result = new AdaptiveThetaJoinComponent(firstParent,
					secondParent);
		} else if (thetaJoinType == SystemParameters.STATIC_CS) {
			result = new ThetaJoinComponent(firstParent, secondParent,
					true);
		} else if (thetaJoinType == SystemParameters.EPOCHS_CS) {
			result = new AdaptiveThetaJoinComponent(firstParent,
					secondParent);
		} else {
			throw new RuntimeException("Unsupported Thtea Join Type");
		}
		queryBuilder.add(result);
		return result;
	}
}
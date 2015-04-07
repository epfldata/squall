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


package ch.epfl.data.squall.thetajoin.adaptive.advisor;

import java.util.Map;

import ch.epfl.data.squall.utilities.SystemParameters;

/**
 * HeuristicAdvisor class that provides decision@SuppressWarnings("serial") s on
 * reducer assignment.
 */
public class TheoreticalAdvisorNew extends Advisor {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// The number of tuples to be received before the first migration.
	private long firstMigration = 1000;

	// Tuple count just after last migration.
	protected long lastMigrationR, lastMigrationS;

	/**
	 * Use this constructor when you want to start with a specific input matrix.
	 * 
	 * @param reducerCount
	 *            Total number of reducers.
	 * @param initialRows
	 *            Number of row splits.
	 * @param initialColumns
	 *            Number of column splits.
	 */
	public TheoreticalAdvisorNew(int reducerCount, int initialRows,
			int initialColumns, Map conf) {
		super(reducerCount, initialRows, initialColumns);

		if (SystemParameters.isExisting(conf, "DIP_FIRST_MIGRATION"))
			firstMigration = SystemParameters.getInt(conf,
					"DIP_FIRST_MIGRATION");
	}

	@Override
	protected Maybe<Action> doMigration() {

		if ((totalRowTuples + totalColumnTuples) < firstMigration)
			return new Maybe<Action>();

		if (deltaR < lastMigrationR && deltaS < lastMigrationS)
			return new Maybe<Action>();

		lastMigrationR = totalRowTuples;
		lastMigrationS = totalColumnTuples;

		deltaR = 0;
		deltaS = 0;

		int nextRows = currentRows, nextColumns = currentColumns;

		double minValue = 1.0 * totalRowTuples / currentRows + 1.0
				* totalColumnTuples / currentColumns;

		// System.err.println("Future window: " + futureWindow);

		for (int n = 1; n <= reducerCount; n *= 2) {
			final double cost = 1.0 * totalRowTuples / n + 1.0
					* totalColumnTuples / (reducerCount / n);

			// System.err.println("Cost of " + n + ": " + cost
			// + " and migration costs: " + migrationCost);

			if (minValue > cost) {
				nextRows = n;
				nextColumns = reducerCount / n;
				minValue = cost;
			}
		}
		if (nextRows != currentRows)
			return new Maybe<Action>(new Migration(reducerCount, currentRows,
					currentColumns, nextRows, nextColumns));
		return new Maybe<Action>();
	}

	@Override
	protected Maybe<Action> doSplit() {
		return new Maybe<Action>();
	}
}

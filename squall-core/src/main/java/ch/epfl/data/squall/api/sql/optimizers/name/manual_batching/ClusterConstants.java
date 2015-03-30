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


package ch.epfl.data.squall.api.sql.optimizers.name.manual_batching;

public class ClusterConstants {

	public static double getDeserTime(int batchSize) {
		if (batchSize <= 16)
			return 0.45;
		else if (batchSize <= 32)
			return 0.55;
		else if (batchSize <= 64)
			return 0.70;
		else if (batchSize <= 128)
			return 0.90;
		else if (batchSize <= 256)
			return 1.04;
		else if (batchSize <= 512)
			return 1.59;
		else if (batchSize <= 1024)
			return 2.49;
		else if (batchSize <= 2048)
			return 5.00;
		else if (batchSize <= 4096)
			return 7.50;
		else if (batchSize <= 8192)
			return 9.50;
		else if (batchSize <= 16384)
			return 12.00;
		else if (batchSize <= 32768)
			return 15.00;
		else if (batchSize <= 65536)
			return 18.00;
		else if (batchSize <= 131072)
			return 21.00;
		else
			throw new RuntimeException(
					"Missing measurements results for serialization for bs = "
							+ batchSize + ".");
	}

	public static double getJoinTime() {
		// return 0.0037;
		return 0.0097;
	}

	public static double getOpTime() {
		// TODO: It works precisely when there is one access to memory (as in
		// AggregateCountOperator),
		// but it is not general enough for any king of operator (selection,
		// projection).
		// Actually, this method is invoked only when no join exists on the
		// node,
		// the only example so far is TPCH4.
		return getReadTime();
	}

	public static double getReadTime() {
		return 0.0015;
	}

	public static double getSerTime(int batchSize) {
		if (batchSize <= 16)
			return 0.15;
		else if (batchSize <= 32)
			return 0.20;
		else if (batchSize <= 64)
			return 0.30;
		else if (batchSize <= 128)
			return 0.40;
		else if (batchSize <= 256)
			return 0.58;
		else if (batchSize <= 512)
			return 0.59;
		else if (batchSize <= 1024)
			return 0.67;
		else if (batchSize <= 2048)
			return 0.80;
		else if (batchSize <= 4096)
			return 0.95;
		else if (batchSize <= 8192)
			return 1.15;
		else if (batchSize <= 16384)
			return 1.40;
		else if (batchSize <= 32768)
			return 1.65;
		else if (batchSize <= 65536)
			return 1.90;
		else if (batchSize <= 131072)
			return 2.20;
		else
			throw new RuntimeException(
					"Missing measurements results for deserialization for bs = "
							+ batchSize + ".");
	}

}
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


package ch.epfl.data.squall.conversion;

public class SumCountConversion implements TypeConversion<SumCount> {
	private static final long serialVersionUID = 1L;

	@Override
	public SumCount fromString(String sc) {
		final String parts[] = sc.split("\\:");
		final Double sum = Double.valueOf(new String(parts[0]));
		final Long count = Long.valueOf(new String(parts[1]));
		return new SumCount(sum, count);
	}

	@Override
	public double getDistance(SumCount bigger, SumCount smaller) {
		return bigger.getAvg() - smaller.getAvg();
	}

	@Override
	public SumCount getInitialValue() {
		return new SumCount(0.0, 0L);
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "SUM_COUNT";
	}

	@Override
	public String toString(SumCount sc) {
		return sc.getSum() + ":" + sc.getCount();
	}

}
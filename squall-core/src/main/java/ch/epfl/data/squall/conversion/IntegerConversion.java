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

public class IntegerConversion implements NumericConversion<Integer> {
	private static final long serialVersionUID = 1L;

	@Override
	public Integer fromDouble(double d) {
		return (int) d;
	}

	@Override
	public Integer fromString(String str) {
		return Integer.valueOf(str);
	}

	@Override
	public double getDistance(Integer bigger, Integer smaller) {
		return bigger.doubleValue() - smaller.doubleValue();
	}

	@Override
	public Integer getInitialValue() {
		return 0;
	}

	@Override
	public Integer getMaxValue() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Integer getMinPositiveValue() {
		return 1;
	}

	@Override
	public Integer getMinValue() {
		return Integer.MIN_VALUE;
	}

	@Override
	public Integer getOffset(Object base, double delta) {
		return (Integer) base + (int) delta;
	}

	@Override
	public Integer minDecrement(Object obj) {
		return (Integer) obj - getMinPositiveValue();
	}

	@Override
	public Integer minIncrement(Object obj) {
		return (Integer) obj + getMinPositiveValue();
	}

	@Override
	public double toDouble(Object obj) {
		final int value = (Integer) obj;
		return value;
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "INTEGER";
	}

	@Override
	public String toString(Integer obj) {
		return obj.toString();
	}
}

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


package ch.epfl.data.squall.types;

import java.util.Random;

public class LongType implements NumericType<Long> {
	private static final long serialVersionUID = 1L;
	private final Random _rnd = new Random();

	@Override
	public Long fromDouble(double d) {
		return (long) d;
	}

	@Override
	public Long fromString(String str) {
		return Long.valueOf(str);
	}

	@Override
	public double getDistance(Long bigger, Long smaller) {
		return bigger.doubleValue() - smaller.doubleValue();
	}

	@Override
	public Long getInitialValue() {
		return 0L;
	}

	@Override
	public Long getMaxValue() {
		return Long.MAX_VALUE;
	}

	@Override
	public Long getMinPositiveValue() {
		return 1L;
	}

	@Override
	public Long getMinValue() {
		return Long.MIN_VALUE;
	}

	@Override
	public Long getOffset(Object base, double delta) {
		return (Long) base + (long) delta;
	}

	@Override
	public Long minDecrement(Object obj) {
		return (Long) obj - getMinPositiveValue();
	}

	@Override
	public Long minIncrement(Object obj) {
		return (Long) obj + getMinPositiveValue();
	}

	@Override
	public double toDouble(Object obj) {
		final long value = (Long) obj;
		return value;
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "LONG";
	}

	@Override
	public String toString(Long obj) {
		return obj.toString();
	}

	@Override
	public Long generateRandomInstance() {
		return _rnd.nextLong();
	}
}
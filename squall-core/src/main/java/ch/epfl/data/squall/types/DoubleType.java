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

public class DoubleType implements NumericType<Double> {
    private static final long serialVersionUID = 1L;
    private final Random _rnd = new Random();

    @Override
    public Double fromDouble(double d) {
	return d;
    }

    @Override
    public Double fromString(String str) {
	return Double.valueOf(str);
    }

    @Override
    public double getDistance(Double bigger, Double smaller) {
	return bigger - smaller;
    }

    @Override
    public Double getInitialValue() {
	return 0.0;
    }

    @Override
    public Double getMaxValue() {
	return Double.MAX_VALUE;
    }

    @Override
    public Double getMinPositiveValue() {
	return Double.MIN_VALUE;
    }

    @Override
    public Double getMinValue() {
	return -1 * Double.MAX_VALUE;
    }

    @Override
    public Double getOffset(Object base, double delta) {
	return (Double) base + delta;
    }

    @Override
    public Double minDecrement(Object obj) {
	return (Double) obj - getMinPositiveValue();
    }

    @Override
    public Double minIncrement(Object obj) {
	return (Double) obj + getMinPositiveValue();
    }

    @Override
    public double toDouble(Object obj) {
	return (Double) obj;
    }

    // for printing(debugging) purposes
    @Override
    public String toString() {
	return "DOUBLE";
    }

    @Override
    public String toString(Double obj) {
	return obj.toString();
    }

    @Override
    public Double generateRandomInstance() {
	return _rnd.nextDouble();
    }

}
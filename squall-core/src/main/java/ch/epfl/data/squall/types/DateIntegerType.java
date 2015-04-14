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

public class DateIntegerType implements NumericType<Integer> {
    public static void main(String[] args) {
	DateIntegerType dcConv = new DateIntegerType();
	System.out.println(dcConv.getDistance(dcConv.getMaxValue(),
		dcConv.getMinValue()));
	System.out.println(dcConv.getDistance(dcConv.getMinValue(),
		dcConv.getMaxValue()));
	System.out.println(dcConv.getDistance(dcConv.getMaxValue(),
		dcConv.getMaxValue()));
	System.out.println(dcConv.getDistance(dcConv.getMinValue(),
		dcConv.getMinValue()));
	System.out.println(dcConv.minIncrement(dcConv.getMinValue()));
	System.out.println(dcConv.minDecrement(dcConv.getMaxValue()));
    }

    private static final long serialVersionUID = 1L;

    private final DateType _dc = new DateType();

    private final Random _rnd = new Random();

    @Override
    public Integer fromDouble(double d) {
	return (int) d;
    }

    @Override
    public Integer fromString(String str) {
	final String[] splits = str.split("-");
	final int year = Integer.parseInt(new String(splits[0])) * 10000;
	final int month = Integer.parseInt(new String(splits[1])) * 100;
	final int day = Integer.parseInt(new String(splits[2]));
	return year + month + day;
    }

    @Override
    public double getDistance(Integer bigger, Integer smaller) {
	return _dc.getDistance(_dc.fromInteger(bigger),
		_dc.fromInteger(smaller));
    }

    @Override
    public Integer getInitialValue() {
	return 0;
    }

    @Override
    public Integer getMaxValue() {
	// return Integer.MAX_VALUE;
	return 20200101;
    }

    @Override
    public Integer getMinPositiveValue() {
	return 1;
    }

    @Override
    public Integer getMinValue() {
	// return Integer.MIN_VALUE;
	return 18000101;
    }

    @Override
    public Integer getOffset(Object base, double delta) {
	return _dc.addDays((Integer) base, (int) delta);
    }

    @Override
    public Integer minDecrement(Object obj) {
	return _dc.addDays((Integer) obj, -1);
    }

    @Override
    public Integer minIncrement(Object obj) {
	return _dc.addDays((Integer) obj, 1);
    }

    @Override
    public double toDouble(Object obj) {
	return (Integer) obj;
    }

    @Override
    public String toString(Integer obj) {
	return obj.toString();
    }

    public String toStringWithDashes(Integer obj) {
	String strDate = obj.toString();
	return strDate.substring(0, 4) + "-" + strDate.substring(4, 6) + "-"
		+ strDate.substring(6, 8);
    }

    @Override
    public Integer generateRandomInstance() {
	final int year = (_rnd.nextInt(2020 - 1800) + 1800) * 10000;
	final int month = (_rnd.nextInt(12)) * 100;
	final int day = _rnd.nextInt(30);
	return year + month + day;
    }

}

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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.utilities.MyUtilities;

public class DateType implements Type<Date> {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(DateType.class);

	private static final String STRING_DATE_FORMAT = "yyyy-MM-dd";
	
	private static DateIntegerType _dt = new DateIntegerType();
	
	private final Random _rnd = new Random();

	// this cannot be static, because a static field with a constructor cannot
	// be serialized
	private final SimpleDateFormat string_format = new SimpleDateFormat(
			STRING_DATE_FORMAT);

	private static final String INT_DATE_FORMAT = "yyyyMMdd";

	// this cannot be static, because a static field with a constructor cannot
	// be serialized
	private final SimpleDateFormat int_format = new SimpleDateFormat(
			INT_DATE_FORMAT);

	public Date addDays(Date date, int days) {
		final Calendar c = Calendar.getInstance();
		final int unit = Calendar.DAY_OF_MONTH;
		c.setTime(date);
		c.add(unit, days);
		return c.getTime();
	}

	public Integer addDays(Integer dateLong, int days) {
		Date base = fromInteger(dateLong);
		Date result = addDays(base, days);
		return toInteger(result);
	}

	public Date fromInteger(Integer dateInt) {
		return fromString(String.valueOf(dateInt), int_format);
	}

	public Date fromLong(Long dateLong) {
		return fromString(String.valueOf(dateLong), int_format);
	}

	@Override
	public Date fromString(String str) {
		return fromString(str, string_format);
	}

	private Date fromString(String str, SimpleDateFormat format) {
		Date date = null;
		try {
			date = format.parse(str);
		} catch (final ParseException pe) {
			final String error = MyUtilities.getStackTrace(pe);
			LOG.info(error);
			throw new RuntimeException("Invalid Date Format for " + str);
		}
		return date;
	}

	@Override
	public double getDistance(Date bigger, Date smaller) {
		/*
		 * final DateTime smallerDT = new DateTime(smaller); final DateTime
		 * biggerDT = new DateTime(bigger); return Days.daysBetween(smallerDT,
		 * biggerDT).getDays();
		 */
		long diff = bigger.getTime() - smaller.getTime();
		return (diff / (24 * 1000 * 60 * 60));
	}

	@Override
	public Date getInitialValue() {
		return new Date();
	}

	public Integer toInteger(Date obj) {
		return Integer.valueOf(int_format.format(obj));
	}

	public Long toLong(Date obj) {
		return Long.valueOf(int_format.format(obj));
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "DATE";
	}

	@Override
	public String toString(Date obj) {
		return string_format.format(obj);
	}

	@Override
	public Date generateRandomInstance() {
		int intDate=0;
		return fromString(_dt.toStringWithDashes(_dt.generateRandomInstance()));
	}
}
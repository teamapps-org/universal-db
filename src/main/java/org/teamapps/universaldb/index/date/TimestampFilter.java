/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.index.date;

import org.teamapps.universaldb.index.numeric.NumericFilter;
import org.teamapps.universaldb.index.numeric.NumericFilterType;

import java.time.*;
import java.util.List;

public class TimestampFilter extends NumericFilter{

	public static TimestampFilter createGreaterFilter(LocalDate date) {
		return new TimestampFilter(NumericFilterType.GREATER, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createGreaterFilter(LocalDateTime date) {
		return new TimestampFilter(NumericFilterType.GREATER, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createGreaterFilter(Instant date) {
		return new TimestampFilter(NumericFilterType.GREATER, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createGreaterEqualsFilter(LocalDate date) {
		return new TimestampFilter(NumericFilterType.GREATER_EQUALS, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createGreaterEqualsFilter(LocalDateTime date) {
		return new TimestampFilter(NumericFilterType.GREATER_EQUALS, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createGreaterEqualsFilter(Instant date) {
		return new TimestampFilter(NumericFilterType.GREATER_EQUALS, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createSmallerFilter(LocalDate date) {
		return new TimestampFilter(NumericFilterType.SMALLER, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createSmallerFilter(LocalDateTime date) {
		return new TimestampFilter(NumericFilterType.SMALLER, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createSmallerFilter(Instant date) {
		return new TimestampFilter(NumericFilterType.SMALLER, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createSmallerEqualsFilter(LocalDate date) {
		return new TimestampFilter(NumericFilterType.SMALLER_EQUALS, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createSmallerEqualsFilter(LocalDateTime date) {
		return new TimestampFilter(NumericFilterType.SMALLER_EQUALS, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createSmallerEqualsFilter(Instant date) {
		return new TimestampFilter(NumericFilterType.SMALLER_EQUALS, convertToEpochSecond(date), null);
	}

	public static TimestampFilter createBetweenFilter(LocalDate date1, LocalDate date2) {
		return new TimestampFilter(NumericFilterType.BETWEEN, convertToEpochSecond(date1), convertToEpochSecond(date2));
	}

	public static TimestampFilter createBetweenFilter(LocalDateTime date1, LocalDateTime date2) {
		return new TimestampFilter(NumericFilterType.BETWEEN, convertToEpochSecond(date1), convertToEpochSecond(date2));
	}

	public static TimestampFilter createBetweenFilter(Instant date1, Instant date2) {
		return new TimestampFilter(NumericFilterType.BETWEEN, convertToEpochSecond(date1), convertToEpochSecond(date2));
	}

	protected TimestampFilter(NumericFilterType filterType, List<Number> values) {
		super(filterType, values);
	}

	protected TimestampFilter(NumericFilterType filterType, Number value1, Number value2) {
		super(filterType, value1, value2);
	}

	private static int convertToEpochSecond(LocalDate date) {
		return (int) date.toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC);
	}

	private static int convertToEpochSecond(LocalDateTime dateTime) {
		return (int) dateTime.toEpochSecond(ZoneOffset.UTC);
	}

	private static int convertToEpochSecond(Instant instant) {
		return (int) (instant.toEpochMilli() / 1000);
	}


}

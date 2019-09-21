/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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

import java.util.List;

public class TimestampFilter extends NumericFilter{

	/*
		AsSeconds
		AsMilliSeconds
		AsInstant
		AsLocalDateTime
	 */

	public static TimestampFilter createEqualsFilter(Number number) {
		return new TimestampFilter(NumericFilterType.EQUALS, number, null);
	}

	public static TimestampFilter createNotEqualsFilter(Number number) {
		return new TimestampFilter(NumericFilterType.NOT_EQUALS, number, null);
	}

	public static TimestampFilter createGreaterFilter(Number number) {
		return new TimestampFilter(NumericFilterType.GREATER, number, null);
	}

	public static TimestampFilter createGreaterEqualsFilter(Number number) {
		return new TimestampFilter(NumericFilterType.GREATER_EQUALS, number, null);
	}

	public static TimestampFilter createSmallerFilter(Number number) {
		return new TimestampFilter(NumericFilterType.SMALLER, number, null);
	}

	public static TimestampFilter createSmallerEqualsFilter(Number number) {
		return new TimestampFilter(NumericFilterType.SMALLER_EQUALS, number, null);
	}

	public static TimestampFilter createBetweenFilter(Number number1, Number number2) {
		return new TimestampFilter(NumericFilterType.BETWEEN, number1, number2);
	}

	public static TimestampFilter createBetweenExclusiveFilter(Number number1, Number number2) {
		return new TimestampFilter(NumericFilterType.BETWEEN_EXCLUSIVE, number1, number2);
	}

	protected TimestampFilter(NumericFilterType filterType, List<Number> values) {
		super(filterType, values);
	}

	protected TimestampFilter(NumericFilterType filterType, Number value1, Number value2) {
		super(filterType, value1, value2);
	}


}

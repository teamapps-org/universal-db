/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
package org.teamapps.universaldb.index.numeric;

import org.teamapps.universaldb.index.enumeration.EnumFilterType;
import org.teamapps.universaldb.pojo.Entity;

import java.util.*;
import java.util.stream.Collectors;

public class NumericFilter {
	
	private NumericFilterType filterType;
	private Number value1;
	private Number value2;
	private List<Number> values;

	
	public static NumericFilter equalsFilter(Number number) {
		return new NumericFilter(NumericFilterType.EQUALS, number, null);
	}

	public static NumericFilter notEqualsFilter(Number number) {
		return new NumericFilter(NumericFilterType.NOT_EQUALS, number, null);
	}

	public static NumericFilter greaterFilter(Number number) {
		return new NumericFilter(NumericFilterType.GREATER, number, null);
	}

	public static NumericFilter greaterEqualsFilter(Number number) {
		return new NumericFilter(NumericFilterType.GREATER_EQUALS, number, null);
	}

	public static NumericFilter smallerFilter(Number number) {
		return new NumericFilter(NumericFilterType.SMALLER, number, null);
	}

	public static NumericFilter smallerEqualsFilter(Number number) {
		return new NumericFilter(NumericFilterType.SMALLER_EQUALS, number, null);
	}

	public static NumericFilter betweenFilter(Number number1, Number number2) {
		return new NumericFilter(NumericFilterType.BETWEEN, number1, number2);
	}

	public static NumericFilter betweenExclusiveFilter(Number number1, Number number2) {
		return new NumericFilter(NumericFilterType.BETWEEN_EXCLUSIVE, number1, number2);
	}

	public static NumericFilter containsFilter(Number ... numbers) {
		return new NumericFilter(NumericFilterType.CONTAINS, Arrays.asList(numbers));
	}

	public static NumericFilter containsEntitiesFilter(Collection<? extends Entity> entities) {
		List<Number> list = entities.stream().map(entity -> (Number) entity.getId()).collect(Collectors.toList());
		return new NumericFilter(NumericFilterType.CONTAINS, list);
	}

	public static NumericFilter containsFilter(List<Number> numbers) {
		return new NumericFilter(NumericFilterType.CONTAINS, numbers);
	}

	public static NumericFilter containsNotFilter(List<Number> numbers) {
		return new NumericFilter(NumericFilterType.CONTAINS_NOT, numbers);
	}

	public static NumericFilter createEnumFilter(EnumFilterType filterType, Enum ... enums) {
		List<Number> enumIds = new ArrayList<>();
		if (enums != null) {
			for (Enum anEnum : enums) {
				enumIds.add((short) (anEnum.ordinal() + 1));
			}
		}
		return createEnumFilter(filterType, enumIds);
	}

	public static NumericFilter createEnumFilter(EnumFilterType filterType, List<Number> enumIds) {
		short value = enumIds.isEmpty() ? 0 : (short) enumIds.get(0);
		switch (filterType) {
			case EQUALS:
				return equalsFilter(value);
			case NOT_EQUALS:
				return notEqualsFilter(value);
			case IS_EMPTY:
				return equalsFilter(0);
			case IS_NOT_EMPTY:
				return notEqualsFilter(0);
			case CONTAINS:
				return containsFilter(enumIds);
			case CONTAINS_NOT:
				return containsNotFilter(enumIds);
		}
		return null;
	}

	protected NumericFilter(NumericFilterType filterType, Number value1, Number value2) {
		this.filterType = filterType;
		this.value1 = value1;
		this.value2 = value2;
	}

	protected NumericFilter(NumericFilterType filterType, List<Number> values) {
		this.filterType = filterType;
		this.values = values;
	}

	public NumericFilterType getFilterType() {
		return filterType;
	}

	public Number getValue1() {
		return value1;
	}

	public Number getValue2() {
		return value2;
	}

	public List<Number> getValues() {
		return values;
	}

	@Override
	public String toString() {
		return filterType + ":" + value1 + "," + value2;
	}
}

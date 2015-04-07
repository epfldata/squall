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


package ch.epfl.data.squall.api.sql.schema;

import java.io.Serializable;

import ch.epfl.data.squall.types.Type;

public class ColumnNameType implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String _name; // Column's fullAliasedName, i.e. N1.NAME
	private final Type _type;

	public ColumnNameType(String name, Type type) {
		_name = name;
		_type = type;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof ColumnNameType))
			return false;
		final ColumnNameType other = (ColumnNameType) obj;
		// we assume names are unique, so it's enough to compare for names
		return _name.equals(other.getName());
	}

	public String getName() {
		return _name;
	}

	public Type getType() {
		return _type;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 97 * hash + (_name != null ? _name.hashCode() : 0);
		hash = 97 * hash + (_type != null ? _type.hashCode() : 0);
		return hash;
	}

	@Override
	public String toString() {
		// return "<" + _name + ", " + _type + ">";
		return _name;
	}
}
package sql.schema;

import java.io.Serializable;

import plan_runner.conversion.TypeConversion;

public class ColumnNameType implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String _name; // Column's fullAliasedName, i.e. N1.NAME
	private final TypeConversion _type;

	public ColumnNameType(String name, TypeConversion type) {
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

	public TypeConversion getType() {
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
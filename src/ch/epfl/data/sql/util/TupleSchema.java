package ch.epfl.data.sql.util;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.sql.schema.ColumnNameType;

/*
 * A list of ColumnNameTypes + list of synonims
 *   obtained when right parent hash indexes are projected away.
 */
public class TupleSchema implements Serializable {
	private static final long serialVersionUID = 1L;

	private final List<ColumnNameType> _cnts;

	// from a synonim to the corresponding column-original
	private Map<String, String> _columnSynonims;

	public TupleSchema(List<ColumnNameType> cnts) {
		_cnts = cnts;
	}

	/*
	 * returns null if nothing is found for this synonimColumn returns null if
	 * this is original column
	 */
	public String getOriginal(Column synonimColumn) {
		if (_columnSynonims == null)
			return null;
		final String colStr = ParserUtil.getStringExpr(synonimColumn);
		return _columnSynonims.get(colStr);
	}

	public List<ColumnNameType> getSchema() {
		return _cnts;
	}

	public Map<String, String> getSynonims() {
		return _columnSynonims;
	}

	public void setSynonims(Map<String, String> columnSynonims) {
		_columnSynonims = columnSynonims;
	}
}
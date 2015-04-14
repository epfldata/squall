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

package ch.epfl.data.squall.api.sql.util;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.squall.api.sql.schema.ColumnNameType;

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
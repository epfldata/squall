
package sql.util;

import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import sql.schema.ColumnNameType;

/*
 * A list of ColumnNameTypes + list of synonims 
 *   obtained when right parent hash indexes are projected away.
 */
public class TupleSchema {
    private List<ColumnNameType> _cnts;
    
    //from a synonim to the corresponding column-original
    private Map<String, Column> _columnSynonims;
    
    public TupleSchema(List<ColumnNameType> cnts){
        _cnts = cnts;
    }
    
    public List<ColumnNameType> getSchema(){
        return _cnts;
    }
    
    public Map<String, Column> getSynonims(){
        return _columnSynonims;
    }
    
    public void setSynonims(Map<String, Column> columnSynonims){
        _columnSynonims = columnSynonims;
    }

    /*
     * returns null if nothing is found for this synonimColumn
     * returns null if this is original column
     */
    public Column getOriginal(Column synonimColumn) {
        if(_columnSynonims == null){
            return null;
        }
        String colStr = ParserUtil.getStringExpr(synonimColumn);
        return _columnSynonims.get(colStr);
    }
}
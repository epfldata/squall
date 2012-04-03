/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.schema.Table;
import schema.Schema;
import util.TableAliasName;
import util.ParserUtil;

public class TableSelector {
    private List<PairTableSize> _pairsTableSize;
    private TableAliasName _tan;

    public TableSelector(List<Table> tableList, Schema schema, TableAliasName tan) {
        _tan = tan;

        //generateSubplan (table, size) list from tables from the query
        _pairsTableSize = createSizePairs(tableList, schema);
        //in place sort
        Collections.sort(_pairsTableSize);
    }

    //best means the smallest available
    public Table removeBestTable(){
        return _pairsTableSize.remove(0).getTable();
    }

    public List<Table> removeAll(){
        List<Table> tableList = new ArrayList<Table>();
        while(!_pairsTableSize.isEmpty()){
            tableList.add(_pairsTableSize.remove(0).getTable());
        }
        return tableList;
    }

    //Best means smallest available from the pairs
    public Table removeBestPairedTable(List<String> joinedWith) {
        for (int i = 0; i < _pairsTableSize.size(); i++) {
            PairTableSize pts = _pairsTableSize.get(i);
            String currentTblName = pts.getTblCompName();
            if (joinedWith.contains(currentTblName)) {
                _pairsTableSize.remove(i);
                return _tan.getTable(currentTblName);
            }
        }
        //all the pairs I can join with are already taken
        return null;
    }

    private List<PairTableSize> createSizePairs(List<Table> tableList, Schema schema) {
        List<PairTableSize> pairsTableSize = new ArrayList<PairTableSize>();
        for(Table table: tableList){
            String schemaName = _tan.getSchemaName(ParserUtil.getComponentName(table));
            int tableSize = schema.getTableSize(schemaName);

            PairTableSize pts = new PairTableSize(table, tableSize);
            pairsTableSize.add(pts);
        }
        return pairsTableSize;
    }

    public int size() {
        return _pairsTableSize.size();
    }

    public class PairTableSize implements Comparable<PairTableSize>{
        private Table _table;
        private int _size;

        public PairTableSize(Table table, int size){
            _table = table;
            _size = size;
        }

        public Table getTable(){
            return _table;
        }

        public String getTblCompName(){
            return ParserUtil.getComponentName(_table);
        }

        public int getSize(){
            return _size;
        }

        @Override
        public int compareTo(PairTableSize t) {
            int otherSize = t.getSize();
            return (new Integer(_size)).compareTo(new Integer(otherSize));
        }
     }

}

package sql.optimizers.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.schema.Table;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.util.TableAliasName;

public class TableSelector {
    private List<PairTableNameSize> _pairsTableNameSize;
    private TableAliasName _tan;

    public TableSelector(List<Table> listTables, Schema schema, TableAliasName tan) {
        _tan = tan;

        //generateSubplan (table, size) list from tables from the query
        _pairsTableNameSize = createSizePairs(listTables, schema);
        //in place sort
        Collections.sort(_pairsTableNameSize);
    }

    //best means the smallest available
    public String removeBestTableName(){
        return _pairsTableNameSize.remove(0).getTableName();
    }

    public List<String> removeAll(){
        List<String> tableNameList = new ArrayList<String>();
        while(!_pairsTableNameSize.isEmpty()){
            tableNameList.add(_pairsTableNameSize.remove(0).getTableName());
        }
        return tableNameList;
    }

    //Best means smallest available from the pairs
    public String removeBestPairedTableName(List<String> joinedWith) {
        for (int i = 0; i < _pairsTableNameSize.size(); i++) {
            PairTableNameSize pts = _pairsTableNameSize.get(i);
            String currentTableName = pts.getTableName();
            if (joinedWith.contains(currentTableName)) {
                _pairsTableNameSize.remove(i);
                return currentTableName;
            }
        }
        //all the pairs I can join with are already taken
        return null;
    }

    private List<PairTableNameSize> createSizePairs(List<Table> listTables, Schema schema) {
        List<PairTableNameSize> pairsTableSize = new ArrayList<PairTableNameSize>();
        for(Table table: listTables){
            String schemaName = _tan.getSchemaName(ParserUtil.getComponentName(table));
            long tableSize = schema.getTableSize(schemaName);

            PairTableNameSize pts = new PairTableNameSize(table, tableSize);
            pairsTableSize.add(pts);
        }
        return pairsTableSize;
    }

    public int size() {
        return _pairsTableNameSize.size();
    }

    public class PairTableNameSize implements Comparable<PairTableNameSize>{
        private String _tableName;
        private long _size;

        public PairTableNameSize(Table table, long size){
            _tableName = ParserUtil.getComponentName(table);
            _size = size;
        }

        public String getTableName(){
            return _tableName;
        }

        public long getSize(){
            return _size;
        }

        @Override
        public int compareTo(PairTableNameSize t) {
            long otherSize = t.getSize();
            return (new Long(_size)).compareTo(new Long(otherSize));
        }
     }

}

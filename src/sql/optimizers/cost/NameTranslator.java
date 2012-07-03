package sql.optimizers.cost;

import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.log4j.Logger;
import plan_runner.conversion.TypeConversion;
import sql.schema.ColumnNameType;
import sql.util.ParserUtil;
import sql.util.TupleSchema;

/*
 * Expressions are being translated to columns from TupleSchema
 * This is explicitly done only in contains and indexOf because all the other methods from this classs call it.
 */
public class NameTranslator{
    private static Logger LOG = Logger.getLogger(NameTranslator.class);
    
    private String _compName; //the name of the component which created it, used only for printing error messages
    
    public NameTranslator(String compName){
        _compName = compName;
    }
    
    /*
     * WRAPPER methods
     */
    public boolean contains(TupleSchema tupleSchema, Expression expr){
        if(indexOf(tupleSchema.getSchema(), ParserUtil.getStringExpr(expr)) != ParserUtil.NOT_FOUND){
            //worked out without using synonims
            return true;
        }
        
        translateExpr(tupleSchema, expr);
        //if after changing synonims still not found, return false
        return indexOf(tupleSchema.getSchema(), ParserUtil.getStringExpr(expr)) != ParserUtil.NOT_FOUND;
    }
    
    /*
     * 
     */
    public int indexOf(TupleSchema tupleSchema, Expression expr){
        int index = indexOf(tupleSchema.getSchema(), ParserUtil.getStringExpr(expr));
        if(index != ParserUtil.NOT_FOUND){
            //worked out without using synonims
            return index;
        }
        
        translateExpr(tupleSchema, expr);
        //if after changing synonims still not found, return -1
        return indexOf(tupleSchema.getSchema(), ParserUtil.getStringExpr(expr));
    }

         
    /*
     * This method differs from getColumnIndex since it might return -1 (and user know how to deal with -1)
     * tupleSchema.getSchema() contains a list of TableAlias.ColumnName
     */
    public int getColumnIndex(TupleSchema tupleSchema, Column column) {
        int index = indexOf(tupleSchema, column);
        if(index == ParserUtil.NOT_FOUND){
            String colStr = ParserUtil.getStringExpr(column);
            LOG.error("Column " + colStr + " cannot be found in " + _compName + " !");
        }
        return index;
    }
    
    /*
     *    List<ColumnNameType> is a user schema with full names (TableAlias.ColumnName)
     *    For a field N1.NATIONNAME, columnName is N1.NATIONNAME
     */
    public TypeConversion getType(TupleSchema tupleSchema, Expression expr){
        int index = indexOf(tupleSchema, expr);
        if(index == ParserUtil.NOT_FOUND){
            String exprStr = ParserUtil.getStringExpr(expr);
            throw new RuntimeException("No column " + exprStr + " in tupleSchema!");
        }
        return tupleSchema.getSchema().get(index).getType();
    }    
    
    
    /*
     * ORIGINAL private methods
     * 
     *   For a field N1.NATIONNAME, columnName is N1.NATIONNAME
     *   List<ColumnNameType> is a user schema with full names (TableAlias.ColumnName)
     */
    private int indexOf(List<ColumnNameType> tupleSchema, String columnName){
        for(int i=0; i<tupleSchema.size(); i++){
            if(tupleSchema.get(i).getName().equals(columnName)){
                return i;
            }
        }
        return ParserUtil.NOT_FOUND;
    }
    
    
    /* TRANSLATOR methods
     * 
     * all the synonim columns are exchanged with our columns
     *   Done in place.
     */
    private void translateExpr(TupleSchema tupleSchema, Expression expr){
        List<Column> columns = ParserUtil.getJSQLColumns(expr);
        for(Column column: columns){
            Column originalColumn = tupleSchema.getOriginal(column);
            if(originalColumn != null){
                setColumn(column, originalColumn);
            }
        }
    }
    
    /*
     * This is a copy of fromColumn to toColumn
     */
    private void setColumn(Column toColumn, Column fromColumn){
        toColumn.setColumnName(fromColumn.getColumnName());
        toColumn.setTable(fromColumn.getTable());
    }    

    
//    Commented out when List<ColumnTypeNames> moved to TupleSchema. 
//    Not needed anyway because of the single-last-node API requirement.
//    If decided to push forward, translation akin to one from contains method might be needed.    
    
//    /*
//     * Is component already hashed by hashIndexes
//     *   (does its parent sends tuples hashed by hashIndexes).
//     *   hashIndexes are indexes wrt component.
//     *
//     * If returns true not only if hashes are equivalent, but also if the parent groups tuples exactly the same as the affected component,
//     *   with addition of some more columns. This means that Join and Aggregation can be performed on the same node.
//     * Inspiration taken from the Nephele paper.
//     *
//     */
//    public boolean isHashedBy(Component component, List<Integer> hashIndexes, Map<String, CostParams> compCost) {
//        //schema of hashIndexes in component
//        List<ColumnNameType> tupleSchema = compCost.get(component.getName()).getSchema();
//        List<ColumnNameType> projectedTupleSchema = ParserUtil.getProjectedSchema(tupleSchema, hashIndexes);
//
//        Component[] parents = component.getParents();
//        if(parents!=null){
//            //if both parents have only hashIndexes, they point to the same indexes in the child
//            //so we choose arbitrarily first parent
//            Component parent = parents[0];
//            List<Integer> parentHashes = parent.getHashIndexes();
//            if(parent.getHashExpressions() == null){
//                List<ColumnNameType> parentTupleSchema = compCost.get(parent.getName()).getSchema();
//                List<ColumnNameType> projectedParentTupleSchema = ParserUtil.getProjectedSchema(parentTupleSchema, parentHashes);
//                return isSuperset(projectedParentTupleSchema, projectedTupleSchema);
//            }
//        }
//        return false;
//    }
//
//    private boolean isSuperset(List<ColumnNameType> parentHashSchema, List<ColumnNameType> hashSchema) {
//        int parentSize = parentHashSchema.size();
//        int affectedSize = hashSchema.size();
//
//        if (parentSize < affectedSize){
//            return false;
//        }else if(parentSize == affectedSize){
//            return parentHashSchema.equals(hashSchema);
//        }else{
//            //parent partitions more than necessary for a child
//            for(int i=0; i<affectedSize; i++){
//                if (!(hashSchema.get(i).equals(parentHashSchema.get(i)))){
//                    return false;
//                }
//            }
//            return true;
//        }
//    }
    
}

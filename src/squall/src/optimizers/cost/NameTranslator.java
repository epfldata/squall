package optimizers.cost;

import components.Component;
import conversion.TypeConversion;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import optimizers.Translator;
import queryPlans.QueryPlan;
import schema.ColumnNameType;
import util.ParserUtil;


public class NameTranslator implements Translator{

    @Override
    public boolean contains(List<ColumnNameType> tupleSchema, String columnName){
        for(ColumnNameType cnt: tupleSchema){
            if (cnt.getName().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    /*
     *   For a field N1.NATIONNAME, columnName is N1.NATIONNAME
     *   List<ColumnNameType> is a user schema with full names (TableAlias.ColumnName)
     */
    @Override
    public int indexOf(List<ColumnNameType> tupleSchema, String columnName){
        for(int i=0; i<tupleSchema.size(); i++){
            if(tupleSchema.get(i).getName().equals(columnName)){
                return i;
            }
        }
        return -1;
    }

    /*
     *    List<ColumnNameType> is a user schema with full names (TableAlias.ColumnName)
     *    For a field N1.NATIONNAME, columnName is N1.NATIONNAME
     */
    @Override
    public TypeConversion getType(List<ColumnNameType> tupleSchema, String columnName){
        int index = indexOf(tupleSchema, columnName);
        if(index == -1){
            throw new RuntimeException("No column " + columnName + " in tupleSchema!");
        }
        return tupleSchema.get(index).getType();
    }



    /*
     * tupleSchema contains a list of TableAlias.ColumnName
     */
    public int getColumnIndex(Column column, List<ColumnNameType> tupleSchema) {
        String fullAliasedName = ParserUtil.getFullAliasedName(column);
        return indexOf(tupleSchema, fullAliasedName);
    }

    /*
     * Is component already hashed by hashIndexes
     *   (does its parent sends tuples hashed by hashIndexes).
     *   hashIndexes are indexes wrt component.
     *
     * If returns true not only if hashes are equivalent, but also if the parent groups tuples exactly the same as the affected component,
     *   with addition of some more columns. This means that Join and Aggregation can be performed on the same node.
     * Inspiration taken from the Nephele paper.
     *
     */
    public boolean isHashedBy(Component component, List<Integer> hashIndexes, Map<String, CostParams> compCost) {
        //schema of hashIndexes in component
        List<ColumnNameType> tupleSchema = compCost.get(component.getName()).getSchema();
        List<ColumnNameType> projectedTupleSchema = ParserUtil.getProjectedSchema(tupleSchema, hashIndexes);

        Component[] parents = component.getParents();
        if(parents!=null){
            //if both parents have only hashIndexes, they point to the same indexes in the child
            //so we choose arbitrarily first parent
            Component parent = parents[0];
            List<Integer> parentHashes = parent.getHashIndexes();
            if(parent.getHashExpressions() == null){
                List<ColumnNameType> parentTupleSchema = compCost.get(parent.getName()).getSchema();
                List<ColumnNameType> projectedParentTupleSchema = ParserUtil.getProjectedSchema(parentTupleSchema, parentHashes);
                return isSuperset(projectedParentTupleSchema, projectedTupleSchema);
            }
        }
        return false;
    }

    private boolean isSuperset(List<ColumnNameType> parentHashSchema, List<ColumnNameType> hashSchema) {
        int parentSize = parentHashSchema.size();
        int affectedSize = hashSchema.size();

        if (parentSize < affectedSize){
            return false;
        }else if(parentSize == affectedSize){
            return parentHashSchema.equals(hashSchema);
        }else{
            //parent partitions more than necessary for a child
            for(int i=0; i<affectedSize; i++){
                if (!(hashSchema.get(i).equals(parentHashSchema.get(i)))){
                    return false;
                }
            }
            return true;
        }
    }


    
    //non-used methods
    public int getColumnIndex(Column column, Component requestor, QueryPlan queryPlan) {
        throw new UnsupportedOperationException("This method is not ment to be called from NameTranslator");
    }

    public boolean isHashedBy(Component component, List<Integer> hashIndexes) {
        throw new UnsupportedOperationException("This method is not ment to be called from NameTranslator");
    }

}

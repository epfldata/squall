package sql.optimizers;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.conversion.TypeConversion;
import sql.schema.ColumnNameType;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.util.TableAliasName;


public class IndexTranslator{
    private Schema _schema;
    private TableAliasName _tan;

    public IndexTranslator(Schema schema, TableAliasName tan){
        _schema = schema;
        _tan = tan;
    }

    public boolean contains(List<ColumnNameType> tupleSchema, String columnName){
        int index = indexOf(tupleSchema, columnName);
        return index != ParserUtil.NOT_FOUND;
    }

    /*
     *   Not used outside this class.
     *   For a field N1.NATIONNAME, columnName is NATIONNAME
     *   List<ColumnNameType> is from a Schema Table (TPCH.nation)
     */
    public int indexOf(List<ColumnNameType> tupleSchema, String columnName){
        for(int i=0; i<tupleSchema.size(); i++){
            if(tupleSchema.get(i).getName().equals(columnName)){
                return i;
            }
        }
        return ParserUtil.NOT_FOUND;
    }

    /*
     * Not used outside this class
     * TupleSchema is a table schema (TPCH_Schema.customer, for example)
     */
    private TypeConversion getType(List<ColumnNameType> tupleSchema, String columnName) {
        int index = indexOf(tupleSchema, columnName);
        if(index == ParserUtil.NOT_FOUND){
            throw new RuntimeException("No column " + columnName + " in tupleSchema!");
        }
        return tupleSchema.get(index).getType();
    }
    

   /*
    * For a given component and column,
    *   find out the index of that column in a given component.
    * not meant to be used with projections - EarlyProjection is the very last thing done on the plan
    *
    * tupleSchema is not used here (it's used for Cost-based optimizer,
    *   where each component updates the schema after each operator)
    */
    public int getColumnIndex(Column column, Component requestor){
        String columnName = column.getColumnName();
        String tblCompName = ParserUtil.getComponentName(column);
        String tableSchemaName = _tan.getSchemaName(tblCompName);
        List<ColumnNameType> columns = _schema.getTableSchema(tableSchemaName);

        int originalIndex = indexOf(columns, columnName);
        
        //finding originator by name in the list of ancestors
        List<DataSourceComponent> sources = requestor.getAncestorDataSources();
        Component originator = null;
        for(DataSourceComponent source: sources){
            if (source.getName().equals(tblCompName)){
                originator = source;
                break;
            }
        }
        
        if (requestor.equals(originator)){
            return originalIndex;
        }else{
            return getChildIndex(originalIndex, originator, requestor);
        }
    }

    public int getChildIndex(int originalIndex, Component originator, Component requestor){
        Component child = originator.getChild();
        Component[] parents = child.getParents();

        if(child.getChainOperator().getProjection()!=null){
            throw new RuntimeException("Cannot use getChildIndex method on the component with Projection! getOutputSize does not work anymore!");
        }

        int index = originalIndex;

        if(parents.length < 2){
            //no changes, only one parent
            return index;
        }

        //only right parent changes the index
        Component leftParent = parents[0];
        Component rightParent = parents[1];

        if (rightParent.equals(originator)){
            if(!rightParent.getHashIndexes().contains(originalIndex)){
                //requested column is *not* in joinColumns
                int indexesBefore = ParserUtil.getNumElementsBefore(originalIndex, rightParent.getHashIndexes());
                index = ParserUtil.getPreOpsOutputSize(leftParent, _schema, _tan) - indexesBefore + originalIndex;
            }else{
                //requested column is in joinColumns
                //if in the keys have to find lhs index
                int joinIndex = rightParent.getHashIndexes().indexOf(originalIndex);
                index = leftParent.getHashIndexes().get(joinIndex);
            }
        }

        if(child.equals(requestor)){
            return index;
        }else{
            return getChildIndex(index, originator.getChild(), requestor);
        }
    }

    /*
     * Is component already hashed by hashIndexes
     *   (does its parent sends tuples hashed by hashIndexes).
     *   hashIndexes are indexes wrt component.
     *
     * If returns true not only if hashes are equivalent, but also if the parent groups tuples exactly the same as the affected component,
     *   with addition of some more columns. This means that Join and Aggregation can be performed on the same node.
     * Inspiration taken from the Nephele paper.
     */
    public boolean isHashedBy(Component component, List<Integer> hashIndexes) {
        Component[] parents = component.getParents();
        if(parents!=null){
            //if both parents have only hashIndexes, they point to the same indexes in the child
            //so we choose arbitrarily first parent
            Component parent = parents[0];
            List<Integer> parentHashes = parent.getHashIndexes();
            if(parent.getHashExpressions() == null){
                List<Integer> parentHashIndexes = new ArrayList<Integer>();
                for(int parentHash: parentHashes){
                    parentHashIndexes.add(getChildIndex(parentHash, parent, component));
                }
                return isSuperset(parentHashIndexes, hashIndexes);
            }
        }
        //if there are HashExpressions, we don't bother to do analysis, we know it's false
        return false;
    }

    private boolean isSuperset(List<Integer> parentHashIndexes, List<Integer> affectedHashIndexes) {
        int parentSize = parentHashIndexes.size();
        int affectedSize = affectedHashIndexes.size();

        if (parentSize < affectedSize){
            return false;
        }else if(parentSize == affectedSize){
            return parentHashIndexes.equals(affectedHashIndexes);
        }else{
            //parent partitions more than necessary for a child
            for(int i=0; i<affectedSize; i++){
                if (!(affectedHashIndexes.get(i).equals(parentHashIndexes.get(i)))){
                    return false;
                }
            }
            return true;
        }
    }

}

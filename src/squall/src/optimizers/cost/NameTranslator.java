/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.cost;

import components.Component;
import conversion.TypeConversion;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import optimizers.Translator;
import queryPlans.QueryPlan;
import schema.ColumnNameType;
import util.ParserUtil;


public class NameTranslator implements Translator{

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

    public boolean isHashedBy(Component component, List<Integer> hashIndexes) {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    
    //non-used methods
    public int getColumnIndex(Column column, Component requestor, QueryPlan queryPlan) {
        throw new UnsupportedOperationException("This method is not ment to be called from NameTranslator");
    }

}

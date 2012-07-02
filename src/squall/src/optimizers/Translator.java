package optimizers;

import components.Component;
import conversion.TypeConversion;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import queryPlans.QueryPlan;
import schema.ColumnNameType;


/*
 * This interface contains optimizer-dependent JSQL-Squall translate methods
 */
public interface Translator {

    /*
     * General purpose
     */
    public boolean contains(List<ColumnNameType> tupleSchema, String columnName);
    public int indexOf(List<ColumnNameType> tupleSchema, String columnName);
    public TypeConversion getType(List<ColumnNameType> tupleSchema, String columnName);


    /*
     * Used for IndexTranslator
     * For a given component and column,
     *   find out the index of that column in a given component
     */
    public int getColumnIndex(Column column, Component requestor);

    /*
     * Is component already hashed by hashIndexes
     *   (does its parent sends tuples hashed by hashIndexes).
     *   hashIndexes are indexes wrt component.
     *
     * If returns true not only if hashes are equivalent, but also if the parent groups tuples exactly the same as the affected component,
     *   with addition of some more columns. This means that Join and Aggregation can be performed on the same node.
     * Inspiration taken from the Nephele paper.
     */
    public boolean isHashedBy(Component component, List<Integer> hashIndexes);

    
    /*
     * Used for NameTranslator
     */
    public int getColumnIndex(Column column, List<ColumnNameType> tupleSchema);

}

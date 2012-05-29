/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import components.Component;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import queryPlans.QueryPlan;
import schema.Schema;
import util.TableAliasName;


/*
 * This interface contains optimizer-dependent JSQL-Squall translate methods
 */
public interface OptimizerTranslator {

    /*
    * For a given component and column,
    *   find out the index of that column in a given component
    */
    int getColumnIndex(Column column, Component requestor, QueryPlan queryPlan, Schema schema, TableAliasName tan);

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

}

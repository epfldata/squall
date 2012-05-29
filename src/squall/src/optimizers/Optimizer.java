/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;


public interface Optimizer {

    public ComponentGenerator generate(List<Table> tableList, List<Join> joinList, List<SelectItem> selectItems, Expression whereExpr);

}
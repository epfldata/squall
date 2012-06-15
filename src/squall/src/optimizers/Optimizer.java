package optimizers;

import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;
import queryPlans.QueryPlan;


public interface Optimizer {

    public QueryPlan generate(List<Table> tableList, List<Join> joinList, List<SelectItem> selectItems, Expression whereExpr);

}
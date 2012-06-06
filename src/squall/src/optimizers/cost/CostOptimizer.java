/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.cost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;
import optimizers.IndexComponentGenerator;
import optimizers.Optimizer;
import optimizers.OptimizerTranslator;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;
import visitors.jsql.AndVisitor;

public class CostOptimizer implements Optimizer {
    private Schema _schema;
    private String _dataPath;
    private String _extension;
    private TableAliasName _tan;
    private IndexComponentGenerator _cg;
    private OptimizerTranslator _ot;
    private Map _map; //map is updates in place
    
    HashMap<String, Expression> _compNamesAndExprs = new HashMap<String, Expression>();
    HashMap<Set<String>, Expression> _compNamesOrExprs = new HashMap<Set<String>, Expression>();
    
    public CostOptimizer(Schema schema, TableAliasName tan, String dataPath, String extension, OptimizerTranslator ot, Map map){
        _schema = schema;
        _tan = tan;
        _dataPath = dataPath;
        _extension = extension;
        _ot = ot;
        _map = map;
    }

    public IndexComponentGenerator generate(List<Table> tableList, List<Join> joinList, List<SelectItem> selectItems, Expression whereExpr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /*************************************************************************************
     * SELECT/WHERE visitors
     *************************************************************************************/
    private void processWhereClause(Expression whereExpr) {
        // TODO: in non-nested case, there is a single Expression
        if (whereExpr == null) return;

        AndVisitor andVisitor = new AndVisitor();
        whereExpr.accept(andVisitor);
        List<Expression> atomicAndExprs = andVisitor.getAtomicExprs();
        List<OrExpression> orExprs = andVisitor.getOrExprs();

        /*
         * we have to group atomicExpr (conjuctive terms) by ComponentName
         *   there might be mutliple columns from a single DataSourceComponent, and we want to group them
         *
         * conditions such as R.A + R.B = 10 are possible
         *   not possible to have ColumnReference from multiple tables,
         *   because than it would be join condition
         */
        ParserUtil.addAndExprsToComps(_compNamesAndExprs, atomicAndExprs);
        ParserUtil.addOrExprsToComps(_compNamesOrExprs, orExprs);
    }

}

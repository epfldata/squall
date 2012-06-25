package optimizers.cost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import optimizers.Optimizer;
import queryPlans.QueryPlan;
import schema.Schema;
import schema.TPCH_Schema;
import util.JoinTablesExprs;
import util.ParserUtil;
import utilities.SystemParameters;
import visitors.jsql.AndVisitor;
import visitors.jsql.JoinTablesExprsVisitor;
import visitors.jsql.SQLVisitor;

/*
 * It generates different NameComponentGenerator for each partial query plan
 *   NameComponentGenerator is responsible for attaching operators to components
 * Aggregation only on the last level.
 */
public class CostOptimizer implements Optimizer {
    private Schema _schema;
    private SQLVisitor _pq;
    private Map _map; //map is updates in place
    private int _totalSourcePar;

    private HashMap<String, Expression> _compNamesAndExprs = new HashMap<String, Expression>();
    private HashMap<Set<String>, Expression> _compNamesOrExprs = new HashMap<Set<String>, Expression>();
    
    public CostOptimizer(SQLVisitor pq, Map map, int totalSourcePar){
        _pq = pq;
        _map = map;
        _totalSourcePar = totalSourcePar;
        
        double scallingFactor = SystemParameters.getDouble(map, "DIP_DB_SIZE");
        _schema = new TPCH_Schema(scallingFactor);
    }

    public QueryPlan generate() {
        processWhereClause(_pq.getWhereExpr());
        ProjGlobalCollect globalCollect = new ProjGlobalCollect(_pq.getSelectItems(), _pq.getWhereExpr());
        globalCollect.process();

        //From a list of joins, create collection of elements like {R->{S, R.A=S.A}}
        JoinTablesExprsVisitor jteVisitor = new JoinTablesExprsVisitor();
        for(Join join: _pq.getJoinList()){
            join.getOnExpression().accept(jteVisitor);
        }
        JoinTablesExprs jte = jteVisitor.getJoinTablesExp();


        //INITIAL PARALLELISM has to be computed after previous lines, because they initialize some variables
        //DataSource component has to compute cardinality (WhereClause changes it)
        //  and to set up projections (globalCollect and jte)
        List<Table> tableList = _pq.getTableList();
        CostParallelismAssigner parAssigner = new CostParallelismAssigner(_schema, _pq.getTan(),
                 _map, _compNamesAndExprs, _compNamesOrExprs, globalCollect, jte);
        Map<String, Integer> sourceParallelism = parAssigner.getSourceParallelism(tableList, _totalSourcePar);

        return null;
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
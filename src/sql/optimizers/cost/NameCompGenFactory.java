package sql.optimizers.cost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import plan_runner.utilities.SystemParameters;
import sql.schema.Schema;
import sql.schema.TPCH_Schema;
import sql.util.ParserUtil;
import sql.visitors.jsql.AndVisitor;
import sql.visitors.jsql.SQLVisitor;

/*
 * It generates different NameCompGen for each partial query plan
 *   NameCompGen is responsible for attaching operators to components
 * Aggregation only on the last level.
 */
public class NameCompGenFactory {
    private Schema _schema;
    private SQLVisitor _pq;
    private Map _map; //map is updates in place
    
    private CostParallelismAssigner _parAssigner;
    private ProjGlobalCollect _globalCollect;

    private HashMap<String, Expression> _compNamesAndExprs = new HashMap<String, Expression>();
    private HashMap<Set<String>, Expression> _compNamesOrExprs = new HashMap<Set<String>, Expression>();
    
    /*
     * only plan, no parallelism
     */
    public NameCompGenFactory(SQLVisitor pq, Map map){
        _pq = pq;
        _map = map;
        init();
    }
    
    /*
     * generating plan + parallelism
     */
    public NameCompGenFactory(SQLVisitor pq, Map map, int totalSourcePar){
        this(pq, map);
        setParAssignerMode(totalSourcePar);
    }
    
    private void init(){
        //we need to compute cardinalities (WHERE clause) before instantiating CPA
        processWhereClause(_pq.getWhereExpr());
        _globalCollect = new ProjGlobalCollect(_pq.getSelectItems(), _pq.getWhereExpr());
        _globalCollect.process();

        double scallingFactor = SystemParameters.getDouble(_map, "DIP_DB_SIZE");
        _schema = new TPCH_Schema(scallingFactor);
    }
    
    public final void setParAssignerMode(int totalSourcePar){
        //in general there might be many NameComponentGenerators, 
        //  that's why CPA is computed before of NCG
        _parAssigner = new CostParallelismAssigner(_schema, _pq,
                 _map, _compNamesAndExprs, _compNamesOrExprs, _globalCollect);
        
        //for the same _parAssigner, we might try with different totalSourcePar
        _parAssigner.computeSourcePar(totalSourcePar);
    }
    
    //can be useful when manually specifying the order of joins
    public NameCompGen generate(){
        return new NameCompGen(_schema, _pq,
                 _map, _parAssigner, _compNamesAndExprs, _compNamesOrExprs, _globalCollect);
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
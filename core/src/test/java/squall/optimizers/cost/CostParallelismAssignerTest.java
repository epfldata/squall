package optimizers.cost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.Test;

import ch.epfl.data.sql.main.ParserMain;
import ch.epfl.data.sql.optimizers.name.CostParallelismAssigner;
import ch.epfl.data.sql.optimizers.name.ProjGlobalCollect;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.visitors.jsql.AndVisitor;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;

/**
 *
 * @author vitorovi
 */
public class CostParallelismAssignerTest {
	private static Logger LOG = Logger.getLogger(CostParallelismAssignerTest.class);
	
    private SQLVisitor _parsedQuery;
    private CostParallelismAssigner _cpa;
    
    private HashMap<String, Expression> _compNamesAndExprs = new HashMap<String, Expression>();
    private HashMap<Set<String>, Expression> _compNamesOrExprs = new HashMap<Set<String>, Expression>();    
    
    public CostParallelismAssignerTest() {      
        String parserConfPath = getClass().getResource("/squall/confs/0_1G_tpch7_ncl").getPath();
        ParserMain pm = new ParserMain();
        Map map = pm.createConfig(parserConfPath);
        _parsedQuery = ParserUtil.parseQuery(map);
        
        Schema schema = new Schema(map);
        
        ProjGlobalCollect globalProject = new ProjGlobalCollect(_parsedQuery.getSelectItems(), _parsedQuery.getWhereExpr());
        globalProject.process();
        
        processWhereClause(_parsedQuery.getWhereExpr());
        
        _cpa = new CostParallelismAssigner(schema, _parsedQuery.getTan(), map);
    }
    
    @Test(expected=RuntimeException.class)
    public void testGetSourceParallelism5() {
        LOG.info("test CPA: getSourceParallelism 5");
        _cpa.computeSourcePar(5);     
    }    

    @Test
    public void testGetSourceParallelism6() {
        LOG.info("test CPA: getSourceParallelism 6");
        
        Map<String, Integer> sourceParallelism = _cpa.computeSourcePar(6);     
        Map<String, Integer> expSourceParallelism = new HashMap<String, Integer>(){{
            put("N1", 1);
            put("N2", 1);
            put("CUSTOMER", 1);
            put("SUPPLIER", 1);
            put("ORDERS", 1);
            put("LINEITEM", 1);
        }};
        assertEquals(expSourceParallelism, sourceParallelism);
    }     

    @Test
    public void testGetSourceParallelism15() {
        LOG.info("test CPA: getSourceParallelism 15");
        
        Map<String, Integer> sourceParallelism = _cpa.computeSourcePar(15);     
        Map<String, Integer> expSourceParallelism = new HashMap<String, Integer>(){{
            put("N1", 1);
            put("N2", 1);
            put("CUSTOMER", 1);
            put("SUPPLIER", 1);
            put("ORDERS", 5);
            put("LINEITEM", 6);
        }};
        assertEquals(expSourceParallelism, sourceParallelism);
    }
    
    /**
     * Test of setParallelism method, of class CostParallelismAssigner.
     */
    @Test
    public void testGetSourceParallelism20() {
        LOG.info("test CPA: getSourceParallelism 20");
        
        Map<String, Integer> sourceParallelism = _cpa.computeSourcePar(20);     
        Map<String, Integer> expSourceParallelism = new HashMap<String, Integer>(){{
            put("N1", 1);
            put("N2", 1);
            put("CUSTOMER", 1);
            put("SUPPLIER", 1);
            put("ORDERS", 7);
            put("LINEITEM", 9);
        }};
        assertEquals(expSourceParallelism, sourceParallelism);
    } 
    
    @Test
    public void testGetSourceParallelism50() {
        LOG.info("test CPA: getSourceParallelism 50");
        
        Map<String, Integer> sourceParallelism = _cpa.computeSourcePar(50);
        Map<String, Integer> expSourceParallelism = new HashMap<String, Integer>(){{
            put("N1", 1);
            put("N2", 1);
            put("CUSTOMER", 2);
            put("SUPPLIER", 1);
            put("ORDERS", 21);
            put("LINEITEM", 24);
        }};
        assertEquals(expSourceParallelism, sourceParallelism);
    }    
    
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

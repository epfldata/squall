package sql.optimizers.name;

import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import sql.main.ParserMain;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/**
 *
 * @author vitorovi
 */
public class NameCompGenTest {
    
    public NameCompGenTest() {
    }

    @Test
    public void testDeepCopy() {
        String parserConfPath = CONF_PATH + "0.1G_tpch10_serial";
        NameCompGen original = createCG(parserConfPath);
        original.generateDataSource("CUSTOMER");
        NameCompGen copy = original.deepCopy();
        NameCompGen copy2 = copy.deepCopy();
        original.generateDataSource("ORDERS");
        
        //copy and copy2 should be untouched by the last generateDataSource("ORDERS")
        int copyPlanSize = copy.getQueryBuilder().getPlan().size();
        assertEquals(1, copyPlanSize);
        int copy2PlanSize = copy2.getQueryBuilder().getPlan().size();
        assertEquals(1, copy2PlanSize);
        
        int copyCCSize = copy.getCompCost().size();
        assertEquals(1, copyCCSize);
        int copy2CCSize = copy2.getCompCost().size();
        assertEquals(1, copy2CCSize);
    }
    
    private static final String CONF_PATH = "../test/squall/local/";
    private NameCompGen createCG(String parserConfPath) {
        ParserMain pm = new ParserMain();
        Map map = pm.createConfig(parserConfPath);
        SQLVisitor pq = ParserUtil.parseQuery(map);
        
        NameCompGenFactory factory = new NameCompGenFactory(map, pq.getTan(), 20);
        return factory.create();
    }        
  
}
package estimators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import org.apache.log4j.Logger;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import sql.estimators.SelingerSelectivityEstimator;
import sql.schema.Schema;
import sql.util.TableAliasName;

/**
 *
 * @author vitorovi
 */
public class SelingerSelectivityEstimatorTest {
	private static Logger LOG = Logger.getLogger(SelingerSelectivityEstimatorTest.class);
    private static Column _columnOrderdate;
    private static SelingerSelectivityEstimator _selEstimator; 
    
    public SelingerSelectivityEstimatorTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        Table table = new Table();
        table.setName("ORDERS");
        List<Table> tableList = new ArrayList<Table>(Arrays.asList(table));

        _columnOrderdate = new Column();
        _columnOrderdate.setTable(table);
        _columnOrderdate.setColumnName("ORDERDATE");

        MinorThan mt = new MinorThan();
        mt.setLeftExpression(_columnOrderdate);
        mt.setRightExpression(new DateValue(" 1995-01-01 "));
        
        String path = "../test/squall/schemas/tpch.txt";
        double scallingFactor = 1;
        _selEstimator = new SelingerSelectivityEstimator(new Schema(path, scallingFactor), new TableAliasName(tableList, "SelingerTest"));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    
    @Test
    public void testEstimate_MinorThan() {
        LOG.info("test estimate(MinorThan):");
        MinorThan mt = new MinorThan();
        mt.setLeftExpression(_columnOrderdate);
        mt.setRightExpression(new DateValue("d" + "1995-01-01" + "d"));
        assertEquals("0.45571725571725574", String.valueOf(_selEstimator.estimate(mt)));
    }
}
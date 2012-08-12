package all;


import estimators.JSQLTypeConverterTest;
import estimators.SelingerSelectivityEstimatorTest;
import optimizers.cost.CostOptimizerTest;
import optimizers.cost.CostParallelismAssignerTest;
import optimizers.cost.ProjGlobalCollectTest;
import optimizers.cost.ProjSchemaCreatorTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import sql.schema.parser.SchemaParserTest;
import sql.util.ParserUtilTest;
import visitors.squall.NameSelectItemsVisitorTest;

/**
 *
 * @author vitorovi
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({JSQLTypeConverterTest.class, 
    SelingerSelectivityEstimatorTest.class, 
    ProjGlobalCollectTest.class,
    ProjSchemaCreatorTest.class,
    NameSelectItemsVisitorTest.class,
    CostParallelismAssignerTest.class,
    CostOptimizerTest.class,
    ParserUtilTest.class,
    SchemaParserTest.class
})
public class AllTests {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
}

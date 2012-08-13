package optimizers.cost;

import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.utilities.MyUtilities;
import sql.main.ParserMain;
import sql.optimizers.name.NameCompGen;
import sql.optimizers.name.NameCompGenFactory;
import sql.util.ParserUtil;

/**
 *
 * @author vitorovi
 */
public class CostOptimizerTest {
    private static final String CONF_PATH = "../test/squall/unit_tests/confs/";
    private static final String PLAN_PATH = "../test/squall/unit_tests/printouts/query_plans/";
    
    private Map _map;
    
    public CostOptimizerTest() {
    }
    
    private NameCompGen createCG(String parserConfPath) {
        ParserMain pm = new ParserMain();
        _map = pm.createConfig(parserConfPath);
        
        NameCompGenFactory factory = new NameCompGenFactory(_map, 20);
        return factory.create();
    }    

    
    private NameCompGen createCG(String parserConfPath, int parallelism) {
        ParserMain pm = new ParserMain();
        _map = pm.createConfig(parserConfPath);
        
        NameCompGenFactory factory = new NameCompGenFactory(_map, parallelism);
        return factory.create();
    }    
    
   /*
    * Unfortunately, in test mode we cannot run StormCluster
    *   However we can copy code from any test method here to ParserMain.main, and it will run it
    */
    @Test
    public void testHyracksManual() {
        String parserConfPath = CONF_PATH + "0.1G_hyracks_ncl_serial";
        NameCompGen ncg = createCG(parserConfPath);
        
        DataSourceComponent customerSource = ncg.generateDataSource("CUSTOMER");
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        ncg.generateEquiJoin(customerSource, ordersSource);

        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
       //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "hyracks.plan");
        //System.out.println(expPlanStr);
       
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }
    
    @Test
    public void testTPCH3Manual() {
        String parserConfPath = CONF_PATH + "0.1G_tpch3_ncl_serial";
        NameCompGen ncg = createCG(parserConfPath);
        
        DataSourceComponent customerSource = ncg.generateDataSource("CUSTOMER");
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        Component C_Ojoin = ncg.generateEquiJoin(customerSource, ordersSource);
        DataSourceComponent lineitemSource = ncg.generateDataSource("LINEITEM");
        ncg.generateEquiJoin(C_Ojoin, lineitemSource);
        
        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
        //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "tpch3.plan");
        //System.out.println(expPlanStr);
        
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }
    
    @Test
    public void testTPCH5Manual() {
        String parserConfPath = CONF_PATH + "0.1G_tpch5_ncl_serial";
        NameCompGen ncg = createCG(parserConfPath);
        
        /*
         * R-N-S-L-O-C (this) : 33sec local mode, 51 nodes
         * R-N-S-C-O-L        : 48sec local mode, 83 nodes
         * R-N-S-L-C-O        : never finish, timeout problem, 3783 nodes
         * rule bushy plan    : 51sec local mode, even less total parallelism used
         */
        DataSourceComponent regionSource = ncg.generateDataSource("REGION");
        DataSourceComponent nationSource = ncg.generateDataSource("NATION");
        Component R_Njoin = ncg.generateEquiJoin(regionSource, nationSource);
        DataSourceComponent supplierSource = ncg.generateDataSource("SUPPLIER");
        Component R_N_Sjoin = ncg.generateEquiJoin(R_Njoin, supplierSource);
        DataSourceComponent lineitemSource = ncg.generateDataSource("LINEITEM");
        Component R_N_S_Ljoin = ncg.generateEquiJoin(R_N_Sjoin, lineitemSource);
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        Component R_N_S_L_Ojoin = ncg.generateEquiJoin(R_N_S_Ljoin, ordersSource);
        DataSourceComponent customerSource = ncg.generateDataSource("CUSTOMER");
        ncg.generateEquiJoin(R_N_S_L_Ojoin, customerSource);
        
        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
        //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "tpch5.plan");
        //System.out.println(expPlanStr);
        
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }    
    
    @Test
    public void testTPCH7Manual() {
        String parserConfPath = CONF_PATH + "0.1G_tpch7_ncl_serial";
        
        //maxPar with pushing up Or is 11
        NameCompGen ncg = createCG(parserConfPath, 11);
        
        /*
         * some numbers are not accurate anymore (PushingOr time is accurate)
         * S-N1-L-O-C-N2   : 35sec, 42nodes; PushingOr, 24sec, 24nodes 
         * N2-C-O-L-S-N1   : 38sec, 68nodes
         * Rule lefty plan : 35sec 
         */
        DataSourceComponent supplierSource = ncg.generateDataSource("SUPPLIER");
        DataSourceComponent n1Source = ncg.generateDataSource("N1");
        Component S_N1join = ncg.generateEquiJoin(supplierSource, n1Source);
        DataSourceComponent lineitemSource = ncg.generateDataSource("LINEITEM");
        Component S_N1_Ljoin = ncg.generateEquiJoin(S_N1join, lineitemSource);
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        Component S_N1_L_Ojoin = ncg.generateEquiJoin(S_N1_Ljoin, ordersSource);
        DataSourceComponent customerSource = ncg.generateDataSource("CUSTOMER");
        Component S_N1_L_O_Cjoin = ncg.generateEquiJoin(S_N1_L_Ojoin, customerSource);
        DataSourceComponent n2Source = ncg.generateDataSource("N2");
        ncg.generateEquiJoin(S_N1_L_O_Cjoin, n2Source);
        
        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
        //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "tpch7.plan");
        //System.out.println(expPlanStr);
        
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }    
    
    @Test
    public void testTPCH8Manual() {
        String parserConfPath = CONF_PATH + "0.1G_tpch8_ncl_serial";
        NameCompGen ncg = createCG(parserConfPath);
        
         /*
         * Fixed computeHashSelectivity
         * R-N1-C-O-L-P-S-N2: 49nodes, 30sec
         * R-N1-C-O-L-S-P-N2: 54nodes, 33sec
         * R-N1-C-O-L-S-N2-P: 62nodes, 34sec
         * P-L-S-N2-O-C-N1-R: cannot have parallelism more than one on the last level because of Regionkey
         * Manual bushy plan: 50sec
         */
        DataSourceComponent regionSource = ncg.generateDataSource("REGION");
        DataSourceComponent n1Source = ncg.generateDataSource("N1");
        Component R_N1join = ncg.generateEquiJoin(regionSource, n1Source);
        DataSourceComponent customerSource = ncg.generateDataSource("CUSTOMER");
        Component R_N1_Cjoin = ncg.generateEquiJoin(R_N1join, customerSource);
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        Component R_N1_C_Ojoin = ncg.generateEquiJoin(R_N1_Cjoin, ordersSource);
        DataSourceComponent lineitemSource = ncg.generateDataSource("LINEITEM");
        Component R_N1_C_O_Ljoin = ncg.generateEquiJoin(R_N1_C_Ojoin, lineitemSource);
        DataSourceComponent partSouce = ncg.generateDataSource("PART");
        Component R_N1_C_O_L_Pjoin = ncg.generateEquiJoin(R_N1_C_O_Ljoin, partSouce);
        DataSourceComponent supplierSource = ncg.generateDataSource("SUPPLIER");
        Component R_N1_C_O_L_P_Sjoin = ncg.generateEquiJoin(R_N1_C_O_L_Pjoin, supplierSource);
        DataSourceComponent n2Source = ncg.generateDataSource("N2");
        ncg.generateEquiJoin(R_N1_C_O_L_P_Sjoin, n2Source);
        
        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
        //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "tpch8.plan");
        //System.out.println(expPlanStr);
        
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }    
    
    @Test
    public void testTPCH9Manual() {
        String parserConfPath = CONF_PATH + "0.1G_tpch9_ncl_serial";
        NameCompGen ncg = createCG(parserConfPath);
        
          /*
         * P-L-PS-O-S-N     : 55nodes, 41sec
         * P-L-O-PS-S-N     : 55nodes, 41sec (this is not a c/p error)
         * P-L-S-N-O-PS     : 61nodes, 41sec
         * P-L-S-N-PS-O     : 61nodes, 43sec
         * L-S-N-PS-P-0     : 107nodes, more than 200sec
         * Manual lefty plan: 37sec (parallelism everywhere 1)
         */
        DataSourceComponent partSource = ncg.generateDataSource("PART");
        DataSourceComponent lineitemSource = ncg.generateDataSource("LINEITEM");
        Component P_Ljoin = ncg.generateEquiJoin(partSource, lineitemSource);
        DataSourceComponent partSuppSource = ncg.generateDataSource("PARTSUPP");
        Component P_L_SPjoin = ncg.generateEquiJoin(P_Ljoin, partSuppSource);
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        Component P_L_SP_Ojoin = ncg.generateEquiJoin(P_L_SPjoin, ordersSource);
        DataSourceComponent supplierSource = ncg.generateDataSource("SUPPLIER");
        Component P_L_SP_O_Sjoin = ncg.generateEquiJoin(P_L_SP_Ojoin, supplierSource);
        DataSourceComponent nationSource = ncg.generateDataSource("NATION");
        ncg.generateEquiJoin(P_L_SP_O_Sjoin, nationSource);
        
        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
        //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "tpch9.plan");
        //System.out.println(expPlanStr);
        
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }    
    
    @Test
    public void testTPCH10Manual() {
        String parserConfPath = CONF_PATH + "0.1G_tpch10_ncl_serial";
        NameCompGen ncg = createCG(parserConfPath);
        
        /*
         * L-O-C-N          : 34nodes, 23sec 
         * C-O-N-L          : 35nodes, 23sec. might not work unless schema synonims are set
         * C-N-O-L          : 37nodes, 22sec , might not work unless schema synonims are set
         * Manual lefty plan: 22sec  
         */
        DataSourceComponent lineitemSource = ncg.generateDataSource("LINEITEM");
        DataSourceComponent ordersSource = ncg.generateDataSource("ORDERS");
        Component L_Ojoin = ncg.generateEquiJoin(lineitemSource, ordersSource);
        DataSourceComponent customerSource = ncg.generateDataSource("CUSTOMER");
        Component L_O_Cjoin = ncg.generateEquiJoin(L_Ojoin, customerSource);
        DataSourceComponent nationSource = ncg.generateDataSource("NATION");
        ncg.generateEquiJoin(L_O_Cjoin, nationSource);
        
        String planStr = ParserUtil.toString(ncg.getQueryPlan());
        //System.out.println(planStr);
        
        //parallelism has to be set in _map
        //int totalParallelism = ParserUtil.parallelismToMap(ncg, _map);
        //System.out.println("Total parallelism(without KILLER) is " + totalParallelism);
        //new Main(ncg.getQueryPlan(), _map);
        
        //expected query plan
        String expPlanStr = MyUtilities.readFile(PLAN_PATH + "tpch10.plan");
        //System.out.println(expPlanStr);
        
        //comparing
        assertEquals(expPlanStr.length(), planStr.length());
        assertEquals(expPlanStr, planStr);
    }
   
}
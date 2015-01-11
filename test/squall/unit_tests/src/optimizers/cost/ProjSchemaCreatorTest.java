package optimizers.cost;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.Test;

import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.conversion.*;
import ch.epfl.data.plan_runner.expressions.*;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.sql.main.ParserMain;
import ch.epfl.data.sql.optimizers.name.ProjGlobalCollect;
import ch.epfl.data.sql.optimizers.name.ProjSchemaCreator;
import ch.epfl.data.sql.schema.ColumnNameType;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.util.TupleSchema;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;

/**
 *
 * @author vitorovi
 */
public class ProjSchemaCreatorTest {
	private static Logger LOG = Logger.getLogger(ProjSchemaCreatorTest.class);	
	
        //this will not break any contracts,
    //  even with new DateConversion() on all the places,
    //  we will have a single object per (possibly) multiple spout/bolt threads.
    //generating plans is done from a single thread, static additionally saves space
    private static LongConversion _lc = new LongConversion();
    private static IntegerConversion _ic = new IntegerConversion();
    private static DoubleConversion _dblConv = new DoubleConversion();
    private static DateConversion _dateConv = new DateConversion();
    private static StringConversion _sc = new StringConversion();
    
    private SQLVisitor _parsedQuery;
    private Schema _schema;
    private ProjGlobalCollect _globalProject;
    
    public ProjSchemaCreatorTest() {
        //create object
        String parserConfPath = "../test/squall/unit_tests/confs/0_1G_tpch7_ncl";
        ParserMain pm = new ParserMain();
        Map map = pm.createConfig(parserConfPath);
        _parsedQuery = ParserUtil.parseQuery(map);
        
        _schema = new Schema(map);
        
        _globalProject = new ProjGlobalCollect(_parsedQuery.getSelectItems(), _parsedQuery.getWhereExpr());
        _globalProject.process();
    }
    
    @Test
    public void testFullExprs() {
        LOG.info("test PSC: full expressions in inputTupleSchema");
        
         
        List<ColumnNameType> inputTupleSchema = new ArrayList<ColumnNameType>();
        //from left parent
        inputTupleSchema.add(new ColumnNameType("SUPPLIER.SUPPKEY", _lc));
        inputTupleSchema.add(new ColumnNameType("N1.NAME", _sc));
        //from right parent
        inputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc));
        //inputTupleSchema.add(new ColumnNameType("LINEITEM.SUPPKEY", _lc)); - join key
        inputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)", _dblConv));
        inputTupleSchema.add(new ColumnNameType("EXTRACT_YEAR(LINEITEM.SHIPDATE)", _ic));
        
        EquiJoinComponent L_S_Njoin = createTPCH7_LSNSubplan();

        ProjSchemaCreator psc = new ProjSchemaCreator(_globalProject, new TupleSchema(inputTupleSchema), L_S_Njoin, _parsedQuery, _schema);
        psc.create();
        
        List<ColumnNameType> outputTupleSchema = psc.getOutputSchema().getSchema();
        ProjectOperator projectOperator = psc.getProjectOperator();
         
        //expected results
        List<ColumnNameType> expOutputTupleSchema = new ArrayList<ColumnNameType>();
        expOutputTupleSchema.add(new ColumnNameType("N1.NAME", _sc));
        expOutputTupleSchema.add(new ColumnNameType("EXTRACT_YEAR(LINEITEM.SHIPDATE)", _ic));
        expOutputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)", _dblConv));
        expOutputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc));
        ProjectOperator expProjectOperator = new ProjectOperator(new ColumnReference(_sc, 1, "N1.NAME"), 
                                                            new ColumnReference(_ic, 4, "EXTRACT_YEAR(LINEITEM.SHIPDATE)"), 
                                                            new ColumnReference(_dblConv, 3, "LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)"), 
                                                            new ColumnReference(_ic, 2, "LINEITEM.ORDERKEY") 
                                                                );
        
        //compare it
        assertEquals(expOutputTupleSchema, outputTupleSchema); //ColumnNameType has equals method
        assertEquals(expProjectOperator.toString(), projectOperator.toString());
    }
    
    @Test
    public void testSubexpresssions() {
        LOG.info("test PSC: subexpressions in inputTupleSchema");
        
         
        List<ColumnNameType> inputTupleSchema = new ArrayList<ColumnNameType>();
        //from left parent
        inputTupleSchema.add(new ColumnNameType("SUPPLIER.SUPPKEY", _lc));
        inputTupleSchema.add(new ColumnNameType("N1.NAME", _sc));
        //from right parent
        inputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc));
        //inputTupleSchema.add(new ColumnNameType("LINEITEM.SUPPKEY", _lc)); - join key
        inputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE", _dblConv));
        inputTupleSchema.add(new ColumnNameType("1.0 - LINEITEM.DISCOUNT", _dblConv));
        inputTupleSchema.add(new ColumnNameType("LINEITEM.SHIPDATE", _ic));
        
        EquiJoinComponent L_S_Njoin = createTPCH7_LSNSubplan();

        ProjSchemaCreator psc = new ProjSchemaCreator(_globalProject, new TupleSchema(inputTupleSchema), L_S_Njoin, _parsedQuery, _schema);
        psc.create();
        
        List<ColumnNameType> outputTupleSchema = psc.getOutputSchema().getSchema();
        ProjectOperator projectOperator = psc.getProjectOperator();
         
        //expected results
        List<ColumnNameType> expOutputTupleSchema = new ArrayList<ColumnNameType>();
        expOutputTupleSchema.add(new ColumnNameType("N1.NAME", _sc)); //1
        expOutputTupleSchema.add(new ColumnNameType("EXTRACT_YEAR(LINEITEM.SHIPDATE)", _ic)); //5
        expOutputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)", _dblConv)); //3, 4
        expOutputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc)); //2
        ProjectOperator expProjectOperator = new ProjectOperator(new ColumnReference(_sc, 1, "N1.NAME"), 
                                                                new IntegerYearFromDate(new ColumnReference(_dateConv, 5, "LINEITEM.SHIPDATE")),
                                                                new Multiplication(
                                                                    new ColumnReference(_dblConv, 3, "LINEITEM.EXTENDEDPRICE"),
                                                                    new ColumnReference(_dblConv, 4, "1.0 - LINEITEM.DISCOUNT")
                                                                ),
                                                                new ColumnReference(_sc, 2, "LINEITEM.ORDERKEY"));
        
        //compare it
        assertEquals(expOutputTupleSchema, outputTupleSchema); //ColumnNameType has equals method
        assertEquals(expProjectOperator.toString(), projectOperator.toString());
    }
    
    @Test
    public void testColumnOnly() {
        LOG.info("test PSC: only columns in inputTupleSchema");
        
         
        List<ColumnNameType> inputTupleSchema = new ArrayList<ColumnNameType>();
        //from left parent
        inputTupleSchema.add(new ColumnNameType("SUPPLIER.SUPPKEY", _lc));
        inputTupleSchema.add(new ColumnNameType("N1.NAME", _sc));
        //from right parent
        inputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc));
        //inputTupleSchema.add(new ColumnNameType("LINEITEM.SUPPKEY", _lc)); - join key
        inputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE", _dblConv));
        inputTupleSchema.add(new ColumnNameType("LINEITEM.DISCOUNT", _dblConv));
        inputTupleSchema.add(new ColumnNameType("LINEITEM.SHIPDATE", _ic));
        
        EquiJoinComponent L_S_Njoin = createTPCH7_LSNSubplan();

        ProjSchemaCreator psc = new ProjSchemaCreator(_globalProject, new TupleSchema(inputTupleSchema), L_S_Njoin, _parsedQuery, _schema);
        psc.create();
        
        List<ColumnNameType> outputTupleSchema = psc.getOutputSchema().getSchema();
        ProjectOperator projectOperator = psc.getProjectOperator();
         
        //expected results
        List<ColumnNameType> expOutputTupleSchema = new ArrayList<ColumnNameType>();
        expOutputTupleSchema.add(new ColumnNameType("N1.NAME", _sc)); //1
        expOutputTupleSchema.add(new ColumnNameType("EXTRACT_YEAR(LINEITEM.SHIPDATE)", _ic)); //5
        expOutputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)", _dblConv)); //3, 4
        expOutputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc)); //2
        ProjectOperator expProjectOperator = new ProjectOperator(new ColumnReference(_sc, 1, "N1.NAME"), 
                                                                new IntegerYearFromDate(new ColumnReference(_dateConv, 5, "LINEITEM.SHIPDATE")),
                                                                new Multiplication(
                                                                    new ColumnReference(_dblConv, 3, "LINEITEM.EXTENDEDPRICE"),
                                                                    new Subtraction(
                                                                        new ValueSpecification(_dblConv, "1.0"),
                                                                        new ColumnReference(_dblConv, 4, "LINEITEM.DISCOUNT")
                                                                    )
                                                                ),
                                                                new ColumnReference(_sc, 2, "LINEITEM.ORDERKEY"));
        
        //compare it
        assertEquals(expOutputTupleSchema, outputTupleSchema); //ColumnNameType has equals method
        assertEquals(expProjectOperator.toString(), projectOperator.toString());
    }    

    //copied from queryPlans.TPCH7, but simplified, only ancestor DataSourceComponent names are used
    private EquiJoinComponent createTPCH7_LSNSubplan() {
        //not used, but has to be initialized
        String dataPath = ""; String extension = "";
        QueryBuilder queryBuilder = new QueryBuilder();

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension);
        queryBuilder.add(relationSupplier);

        //-------------------------------------------------------------------------------------

        DataSourceComponent relationNation1 = new DataSourceComponent(
                "N1",
                dataPath + "nation" + extension);
        queryBuilder.add(relationNation1);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent S_Njoin = new EquiJoinComponent(
                relationSupplier,
                relationNation1);
        queryBuilder.add(S_Njoin);

       //-------------------------------------------------------------------------------------
        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension);
        queryBuilder.add(relationLineitem);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent L_S_Njoin = new EquiJoinComponent(
                relationLineitem,
                S_Njoin);
        queryBuilder.add(L_S_Njoin);
        
        return L_S_Njoin;
    }
}

package visitors.squall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.statement.select.SelectItem;
import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.Test;
import plan_runner.components.DataSourceComponent;
import plan_runner.conversion.*;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.query_plans.QueryBuilder;
import sql.main.ParserMain;
import sql.schema.ColumnNameType;
import sql.util.ParserUtil;
import sql.util.TupleSchema;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.NameSelectItemsVisitor;

/**
 *
 * @author vitorovi
 */
public class NameSelectItemsVisitorTest {
	private static Logger LOG = Logger.getLogger(NameSelectItemsVisitorTest.class);		
	
        //this will not break any contracts,
    //  even with new DateConversion() on all the places,
    //  we will have a single object per (possibly) multiple spout/bolt threads.
    //generating plans is done from a single thread, static additionally saves space
    private static LongConversion _lc = new LongConversion();
    private static IntegerConversion _ic = new IntegerConversion();
    private static DoubleConversion _dblConv = new DoubleConversion();
    private static DateConversion _dateConv = new DateConversion();
    private static StringConversion _sc = new StringConversion();
    
    private SQLVisitor _pq;
    private Map _map; 
    
    public NameSelectItemsVisitorTest() {
        //create object
        String parserConfPath = "../test/squall/unit_tests/confs/0_1G_tpch7_ncl";
        ParserMain pm = new ParserMain();
        _map = pm.createConfig(parserConfPath);
        _pq = ParserUtil.parseQuery(_map);
    }

    @Test
    public void testFullExprs() {
        LOG.info("test NSIV: full expressions in inputTupleSchema");
        
        List<ColumnNameType> inputTupleSchema = new ArrayList<ColumnNameType>();
        inputTupleSchema.add(new ColumnNameType("N1.NAME", _sc));
        inputTupleSchema.add(new ColumnNameType("N2.NAME", _sc));
        inputTupleSchema.add(new ColumnNameType("EXTRACT_YEAR(LINEITEM.SHIPDATE)", _ic));
        inputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)", _dblConv));
        inputTupleSchema.add(new ColumnNameType("LINEITEM.ORDERKEY", _lc)); // not used
        
        DataSourceComponent source = new DataSourceComponent("TestFullExprs", "", new QueryBuilder());
        
        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(new TupleSchema(inputTupleSchema), _map, source);
        for(SelectItem elem: _pq.getSelectItems()){
            elem.accept(selectVisitor);
        }
        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();
        
        //expected
        AggregateOperator agg = new AggregateSumOperator(
                new ColumnReference(_dblConv, 3, "LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)"), 
                _map);
        List<AggregateOperator> expAggOps = Arrays.asList(agg);
        List<ColumnReference> expGroupByVEs = Arrays.asList(new ColumnReference(_sc, 0, "N1.NAME"), 
                                                        new ColumnReference(_sc, 1, "N2.NAME"), 
                                                        new ColumnReference(_ic, 2, "EXTRACT_YEAR(LINEITEM.SHIPDATE)"));
        
        //compare
        assertEquals(expAggOps.toString(), aggOps.toString());
        assertEquals(expGroupByVEs.toString(), groupByVEs.toString());
    }
    
//    @Test
//    public void testSubexpressions() {
//        LOG.info("test NSIV: subexpressions in inputTupleSchema");
//        
//        List<ColumnNameType> inputTupleSchema = new ArrayList<ColumnNameType>();
//        inputTupleSchema.add(new ColumnNameType("N1.NAME", _sc)); //0
//        inputTupleSchema.add(new ColumnNameType("N2.NAME", _sc)); //1
//        inputTupleSchema.add(new ColumnNameType("LINEITEM.SHIPDATE", _ic)); //2
//        inputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE", _dblConv)); //3
//        inputTupleSchema.add(new ColumnNameType("1.0 - LINEITEM.DISCOUNT", _dblConv)); //4
//        
//        DataSourceComponent source = new DataSourceComponent("TestSubExpression", "", new QueryPlan());
//        
//        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(new TupleSchema(inputTupleSchema), _map, source);
//        for(SelectItem elem: _pq.getSelectItems()){
//            elem.accept(selectVisitor);
//        }
//        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
//        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();
//        
//        //expected
//        AggregateOperator agg = new AggregateSumOperator(_dblConv, 
//                                                    new Multiplication(_dblConv, 
//                                                        new ColumnReference(_dblConv, 3, "LINEITEM.EXTENDEDPRICE"),
//                                                        new ColumnReference(_dblConv, 4, "1.0 - LINEITEM.DISCOUNT")
//                                                    ), 
//                                                    _map);
//        
//        List<AggregateOperator> expAggOps = Arrays.asList(agg);
//        List expGroupByVEs = Arrays.asList(new ColumnReference(_sc, 0, "N1.NAME"), 
//                                           new ColumnReference(_sc, 1, "N2.NAME"), 
//                                           new IntegerYearFromDate(new ColumnReference(_dateConv, 2, "LINEITEM.SHIPDATE")));
//        
//        //compare
//        assertEquals(expAggOps.toString(), aggOps.toString());
//        assertEquals(expGroupByVEs.toString(), groupByVEs.toString());
//    }        
//    
//    @Test
//    public void testColumnsOnly() {
//        LOG.info("test NSIV: columns only in inputTupleSchema");
//        
//        List<ColumnNameType> inputTupleSchema = new ArrayList<ColumnNameType>();
//        inputTupleSchema.add(new ColumnNameType("N1.NAME", _sc)); //0
//        inputTupleSchema.add(new ColumnNameType("N2.NAME", _sc)); //1
//        inputTupleSchema.add(new ColumnNameType("LINEITEM.SHIPDATE", _ic)); //2
//        inputTupleSchema.add(new ColumnNameType("LINEITEM.EXTENDEDPRICE", _dblConv)); //3
//        inputTupleSchema.add(new ColumnNameType("LINEITEM.DISCOUNT", _dblConv)); //4
//        
//        DataSourceComponent source = new DataSourceComponent("TestColumnsOnly", "", new QueryPlan());
//        
//        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(new TupleSchema(inputTupleSchema), _map, source);
//        for(SelectItem elem: _pq.getSelectItems()){
//            elem.accept(selectVisitor);
//        }
//        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
//        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();
//        
//        //expected
//        AggregateOperator agg = new AggregateSumOperator(_dblConv, 
//                                                    new Multiplication(_dblConv, 
//                                                        new ColumnReference(_dblConv, 3, "LINEITEM.EXTENDEDPRICE"),
//                                                        new Subtraction(_dblConv,
//                                                            new ValueSpecification(_dblConv, "1.0"),
//                                                            new ColumnReference(_dblConv, 4, "LINEITEM.DISCOUNT")
//                                                        )
//                                                    ), 
//                                                    _map);
//        
//        List<AggregateOperator> expAggOps = Arrays.asList(agg);
//        List expGroupByVEs = Arrays.asList(new ColumnReference(_sc, 0, "N1.NAME"), 
//                                           new ColumnReference(_sc, 1, "N2.NAME"), 
//                                           new IntegerYearFromDate(new ColumnReference(_dateConv, 2, "LINEITEM.SHIPDATE")));
//        
//        //compare
//        assertEquals(expAggOps.toString(), aggOps.toString());
//        assertEquals(expGroupByVEs.toString(), groupByVEs.toString());
//    }    
}

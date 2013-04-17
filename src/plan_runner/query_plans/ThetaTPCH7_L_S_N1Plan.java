package plan_runner.query_plans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.IntegerYearFromDate;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.OrPredicate;

public class ThetaTPCH7_L_S_N1Plan {
	private static Logger LOG = Logger.getLogger(ThetaTPCH7_L_S_N1Plan.class);

	private QueryPlan _queryPlan = new QueryPlan();

	private static final IntegerConversion _ic = new IntegerConversion();

	private static final String    _date1Str = "1995-01-01";
	private static final String    _date2Str = "1996-12-31";
//	private static final String  _firstCountryName = "FRANCE";
//	private static final String _secondCountryName = "GERMANY";
	private static final String  _firstCountryName = "PERU";
	private static final String _secondCountryName = "ETHIOPIA";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date1=_dateConv.fromString(_date1Str);
	private static final Date _date2=_dateConv.fromString(_date2Str);

	public ThetaTPCH7_L_S_N1Plan(String dataPath, String extension, Map conf){		
		//-------------------------------------------------------------------------------------
		ArrayList<Integer> hashSupplier = new ArrayList<Integer>(Arrays.asList(1));

		ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0,3});

		DataSourceComponent relationSupplier = new DataSourceComponent(
				"SUPPLIER",
				dataPath + "supplier" + extension,
				_queryPlan).setHashIndexes(hashSupplier)
				.addOperator(projectionSupplier);

		//-------------------------------------------------------------------------------------
		ArrayList<Integer> hashNation1 = new ArrayList<Integer>(Arrays.asList(1));
		
		SelectOperator selectionNation2 = new SelectOperator(
				new OrPredicate(
						new ComparisonPredicate(
								new ColumnReference(_sc, 1),
								new ValueSpecification(_sc, _firstCountryName)
								), new ComparisonPredicate(
										new ColumnReference(_sc, 1),
										new ValueSpecification(_sc, _secondCountryName)
										)
						));		

		ProjectOperator projectionNation1 = new ProjectOperator(new int[]{1,0});

		DataSourceComponent relationNation1 = new DataSourceComponent(
				"NATION1",
				dataPath + "nation" + extension,
				_queryPlan).setHashIndexes(hashNation1)
				.addOperator(selectionNation2)
				.addOperator(projectionNation1);

		//-------------------------------------------------------------------------------------

		ColumnReference colS = new ColumnReference(_ic, 1);
		ColumnReference colN2 = new ColumnReference(_ic, 1);
		ComparisonPredicate S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colS, colN2);

		Component S_Njoin = new ThetaJoinStaticComponent(
				relationSupplier,
				relationNation1,
				_queryPlan).addOperator(new ProjectOperator(new int[]{0, 2}))
				.setJoinPredicate(S_N_comp);
		//.setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));

		//-------------------------------------------------------------------------------------
		ArrayList<Integer> hashLineitem = new ArrayList<Integer>(Arrays.asList(2));

		SelectOperator selectionLineitem = new SelectOperator(
				new BetweenPredicate(
						new ColumnReference(_dateConv, 10),
						true, new ValueSpecification(_dateConv, _date1),
						true, new ValueSpecification(_dateConv, _date2)
						));

		//first field in projection
		ValueExpression extractYear = new IntegerYearFromDate(
				new ColumnReference<Date>(_dateConv, 10));
		//second field in projection
		//1 - discount
		ValueExpression<Double> substract = new Subtraction(
				new ValueSpecification(_doubleConv, 1.0),
				new ColumnReference(_doubleConv, 6));
		//extendedPrice*(1-discount)
		ValueExpression<Double> product = new Multiplication(
				new ColumnReference(_doubleConv, 5),
				substract);
		//third field in projection
		ColumnReference supplierKey = new ColumnReference(_sc, 2);
		//forth field in projection
		ColumnReference orderKey = new ColumnReference(_sc, 0);
		ProjectOperator projectionLineitem = new ProjectOperator(extractYear, product, supplierKey, orderKey);

		DataSourceComponent relationLineitem = new DataSourceComponent(
				"LINEITEM",
				dataPath + "lineitem" + extension,
				_queryPlan).setHashIndexes(hashLineitem)
				.addOperator(selectionLineitem)
				.addOperator(projectionLineitem);

		//-------------------------------------------------------------------------------------

		ColumnReference colL = new ColumnReference(_ic, 2);
		ColumnReference colS_N = new ColumnReference(_ic, 0);
		ComparisonPredicate L_S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colL, colS_N);
		
		
		
		AggregateCountOperator agg= new AggregateCountOperator(conf);

		Component L_S_Njoin= new ThetaJoinStaticComponent(
				relationLineitem,
				S_Njoin,
				_queryPlan).addOperator(new ProjectOperator(new int[]{5, 0, 1, 3}))
				.setJoinPredicate(L_S_N_comp)
				.addOperator(agg);
				;
		//-------------------------------------------------------------------------------------

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}
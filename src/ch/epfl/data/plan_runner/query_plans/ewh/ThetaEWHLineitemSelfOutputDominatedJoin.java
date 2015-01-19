package ch.epfl.data.plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.plan_runner.conversion.DateIntegerConversion;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateCountOperator;
import ch.epfl.data.plan_runner.operators.PrintOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.AndPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaQueryPlansParameters;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class ThetaEWHLineitemSelfOutputDominatedJoin {
    // never actually tried

    private QueryBuilder _queryBuilder = new QueryBuilder();
    private static final TypeConversion<String> _stringConv = new StringConversion();
    private static final TypeConversion<Integer> _dateIntConv = new DateIntegerConversion();
    private static final IntegerConversion _ic = new IntegerConversion();

    public ThetaEWHLineitemSelfOutputDominatedJoin(String dataPath,
	    String extension, Map conf) {
	// creates materialized relations
	boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
	PrintOperator print1 = printSelected ? new PrintOperator(
		"bci_ewhod_1.tbl", conf) : null;
	PrintOperator print2 = printSelected ? new PrintOperator(
		"bci_ewhod_2.tbl", conf) : null;
	// read from materialized relations
	boolean isMaterialized = SystemParameters.isExisting(conf,
		"DIP_MATERIALIZED")
		&& SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
	boolean isOkcanSampling = SystemParameters.isExisting(conf,
		"DIP_SAMPLING")
		&& SystemParameters.getBoolean(conf, "DIP_SAMPLING");
	boolean isEWHSampling = SystemParameters.isExisting(conf,
		"DIP_EWH_SAMPLING")
		&& SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");

	int quantityBound = 45;

	Component relationLineitem1, relationLineitem2;
	// Project on shipdate , receiptdate, commitdate, shipInstruct, quantity
	ProjectOperator projectionLineitem = new ProjectOperator(new int[] {
		10, 12, 11, 13, 4 });
	final List<Integer> hashLineitem = Arrays.asList(0);

	if (!isMaterialized) {
	    ComparisonPredicate comp11 = new ComparisonPredicate(
		    ComparisonPredicate.EQUAL_OP, new ColumnReference(
			    _stringConv, 14), new ValueSpecification(
			    _stringConv, "TRUCK"));
	    ComparisonPredicate comp12 = new ComparisonPredicate(
		    ComparisonPredicate.GREATER_OP,
		    new ColumnReference(_ic, 4), new ValueSpecification(_ic,
			    quantityBound));

	    AndPredicate and1 = new AndPredicate(comp11, comp12);
	    SelectOperator selectionLineitem1 = new SelectOperator(and1);

	    relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
		    + "lineitem" + extension).add(selectionLineitem1)
		    .add(print1).add(projectionLineitem)
		    .setOutputPartKey(hashLineitem);
	    _queryBuilder.add(relationLineitem1);

	    SelectOperator selectionLineitem2 = new SelectOperator(
		    new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP,
			    new ColumnReference(_stringConv, 14),
			    new ValueSpecification(_stringConv, "TRUCK")));
	    relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
		    + "lineitem" + extension).add(selectionLineitem2)
		    .add(print2).add(projectionLineitem)
		    .setOutputPartKey(hashLineitem);
	    _queryBuilder.add(relationLineitem2);
	} else {
	    relationLineitem1 = new DataSourceComponent("LINEITEM1", dataPath
		    + "bci_ewhod_1" + extension).add(projectionLineitem)
		    .setOutputPartKey(hashLineitem);
	    _queryBuilder.add(relationLineitem1);

	    relationLineitem2 = new DataSourceComponent("LINEITEM2", dataPath
		    + "bci_ewhod_2" + extension).add(projectionLineitem)
		    .setOutputPartKey(hashLineitem);
	    _queryBuilder.add(relationLineitem2);
	}

	NumericConversion keyType = (NumericConversion) _dateIntConv;
	ComparisonPredicate comparison = new ComparisonPredicate(
		ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, 1, keyType);
	int firstKeyProject = 0;
	int secondKeyProject = 0;

	if (printSelected) {
	    relationLineitem1.setPrintOut(false);
	    relationLineitem2.setPrintOut(false);
	} else if (isOkcanSampling) {
	    _queryBuilder = MyUtilities.addOkcanSampler(relationLineitem1,
		    relationLineitem2, firstKeyProject, secondKeyProject,
		    _queryBuilder, keyType, comparison, conf);
	} else if (isEWHSampling) {
	    _queryBuilder = MyUtilities.addEWHSampler(relationLineitem1,
		    relationLineitem2, firstKeyProject, secondKeyProject,
		    _queryBuilder, keyType, comparison, conf);
	} else {
	    int Theta_JoinType = ThetaQueryPlansParameters
		    .getThetaJoinType(conf);
	    boolean isBDB = MyUtilities.isBDB(conf);

	    ColumnReference colLine11 = new ColumnReference(keyType,
		    firstKeyProject); // shipdate
	    // ColumnReference colLine12 = new ColumnReference(_dateConv, 1);
	    // //receiptdate

	    ColumnReference colLine21 = new ColumnReference(keyType,
		    secondKeyProject);
	    // ColumnReference colLine22 = new ColumnReference(_dateConv, 1);

	    // INTERVAL
	    // IntervalPredicate pred3 = new IntervalPredicate(colLine11,
	    // colLine12, colLine22, colLine22);
	    // DateSum add2= new DateSum(colLine22, Calendar.DAY_OF_MONTH, 2);
	    // IntervalPredicate pred4 = new IntervalPredicate(colLine12,
	    // colLine12, colLine22, add2);

	    // B+ TREE or Binary Tree
	    // |col1-col2|<=5

	    ComparisonPredicate pred5 = null;
	    if (!isBDB) {
		pred5 = new ComparisonPredicate(
			ComparisonPredicate.NONGREATER_OP, colLine11,
			colLine21, 1, ComparisonPredicate.BPLUSTREE);
		// ComparisonPredicate pred5 = new
		// ComparisonPredicate(ComparisonPredicate.LESS_OP,colLine11,
		// colLine21, 1, ComparisonPredicate.BPLUSTREE);
		// ComparisonPredicate pred5 = new
		// ComparisonPredicate(ComparisonPredicate.NONGREATER_OP,colLine11,
		// colLine21, 1, ComparisonPredicate.BINARYTREE);
	    } else {
		pred5 = new ComparisonPredicate(
			ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, colLine11,
			colLine21, 1);
	    }

	    AggregateCountOperator agg = new AggregateCountOperator(conf);
	    Component LINEITEMS_LINEITEMSjoin = ThetaJoinComponentFactory
		    .createThetaJoinOperator(Theta_JoinType, relationLineitem1,
			    relationLineitem2, _queryBuilder)
		    .setJoinPredicate(pred5)
		    .setContentSensitiveThetaJoinWrapper(keyType).add(agg);

	    // LINEITEMS_LINEITEMSjoin.setPrintOut(false);
	}

    }

    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }
}
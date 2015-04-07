/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.ewh.examples;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.ewh.components.DummyComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;
import ch.epfl.data.squall.types.DateIntegerType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;

public class ThetaEWHBandOrdersOrderkeyCustkeyJoin {
	private static Logger LOG = Logger
			.getLogger(ThetaEWHBandOrdersOrderkeyCustkeyJoin.class);
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final Type<String> _stringConv = new StringType();
	private static final IntegerType _ic = new IntegerType();
	private DateIntegerType _dic = new DateIntegerType();

	// Bicb
	public ThetaEWHBandOrdersOrderkeyCustkeyJoin(String dataPath,
			String extension, Map conf) {
		// ORDERS * ORDERS on orderkey equi
		// I = 2 * 15M = 30M; O =
		// Variability is [0, 10] * skew
		// baseline z1: MBucket 102s with 8 joiners doing nothing, EWH uses only
		// 3-4 joiners due to large candidate no-output-sample rounded matrix
		// cells
		// B: z1 + firstProject / 10: Output is around 120M tuples. From 283s to
		// 235s (bsp-i). Should compare with 1B as this is output-dominated
		// C(based on A): z2 + secondProject * 10: Output is around 11M tuples.
		// From 140s to 128s (bsp-i).
		// D(based on A): z3 + secondProject * 10: Output is around 11M tuples.
		// From 153s to 147s (bsp-i).
		// BEST: E(based on A): z1 + secondProject * 10, comparisonValue = 2:
		// Output is around 18M tuples. From 152s to 131s (bsp-i), great
		// result!!!.
		// in
		// orders_orderkey_custkey_band/orderkey_custkey_band_16j_z1_abs2_project10
		// A: z1 + secondProject * 10: Output is around 11M tuples. From 120s to
		// 105s (bsp-i), great result!
		// F(based on A): z0 + secondProject * 10, comparisonValue = 2: Output
		// is around 18M tuples. From 134s to 122s (bsp-i).

		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "n_bbosc_1";
		String matName2 = "n_bbosc_2";
		PrintOperator print1 = printSelected ? new PrintOperator(matName1
				+ extension, conf) : null;
		PrintOperator print2 = printSelected ? new PrintOperator(matName2
				+ extension, conf) : null;
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
		boolean isEWHD2Histogram = SystemParameters.getBooleanIfExist(conf,
				HistogramType.D2_COMB_HIST.genConfEntryName());
		boolean isEWHS1Histogram = SystemParameters.getBooleanIfExist(conf,
				HistogramType.S1_RES_HIST.genConfEntryName());
		boolean isSrcHistogram = isEWHD2Histogram || isEWHS1Histogram;

		Component relationOrders1, relationOrders2;
		// Project on shipdate , receiptdate, commitdate, shipInstruct, quantity
		ColumnReference col1 = new ColumnReference(_ic, 0);
		ColumnReference col2 = new ColumnReference(_ic, 2);
		ColumnReference col3 = new ColumnReference(_ic, 3);
		ColumnReference col4 = new ColumnReference(_ic, 4);
		ColumnReference col5 = new ColumnReference(_ic, 5);

		// A
		ColumnReference j1 = new ColumnReference(_ic, 0);
		ValueExpression j2 = new Multiplication(
				new ValueSpecification(_ic, 10), new ColumnReference(_ic, 1));

		// B
		/*
		 * ValueExpression j1 = new Division( new ColumnReference(_ic, 0), new
		 * ValueSpecification(_ic, 10) ); ColumnReference j2 = new
		 * ColumnReference(_ic, 1);
		 */

		ProjectOperator projectionLineitem1 = new ProjectOperator(col1, col2,
				col3, col4, col5, j1);
		ProjectOperator projectionLineitem2 = new ProjectOperator(col1, col2,
				col3, col4, col5, j2);
		// ProjectOperator projectionLineitem1 = new ProjectOperator(new int[]
		// {0, 2, 3, 4, 5, 0});
		// ProjectOperator projectionLineitem2 = new ProjectOperator(new int[]
		// {0, 2, 3, 4, 5, 1});
		final List<Integer> hashLineitem = Arrays.asList(5);

		if (!isMaterialized) {
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ "orders" + extension).add(print1)
					.add(projectionLineitem1).setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders1);

			relationOrders2 = new DataSourceComponent("ORDERS2", dataPath
					+ "orders" + extension).add(print2)
					.add(projectionLineitem2).setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders2);
		} else {
			// WATCH OUT ON PROJECTIONS AFTER MATERIALIZATIONS
			relationOrders1 = new DataSourceComponent("ORDERS1", dataPath
					+ matName1 + extension).add(projectionLineitem1)
					.setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders1);

			relationOrders2 = new DataSourceComponent("LINEITEM2", dataPath
					+ matName2 + extension).add(projectionLineitem2)
					.setOutputPartKey(hashLineitem);
			_queryBuilder.add(relationOrders2);
		}

		NumericType keyType = _ic;
		int comparisonValue = 2;
		if (SystemParameters.isExisting(conf, "COMPARISON_VALUE")) {
			comparisonValue = SystemParameters.getInt(conf, "COMPARISON_VALUE");
			LOG.info("ComparisonValue read from the config file: "
					+ comparisonValue);
		}
		ComparisonPredicate comparison = new ComparisonPredicate(
				ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue,
				keyType);
		int firstKeyProject = 5;
		int secondKeyProject = 5;

		/*
		 * ValueExpression ve1 = new ColumnReference(keyType, firstKeyProject);
		 * ValueExpression ve2 = new Multiplication( new
		 * ValueSpecification(keyType, secondMult), new ColumnReference(keyType,
		 * secondKeyProject)); ProjectOperator project1 = new
		 * ProjectOperator(ve1); ProjectOperator project2 = new
		 * ProjectOperator(ve2);
		 */

		if (printSelected) {
			relationOrders1.setPrintOut(false);
			relationOrders2.setPrintOut(false);
		} else if (isSrcHistogram) {
			_queryBuilder = MyUtilities.addSrcHistogram(relationOrders1,
					firstKeyProject, relationOrders2, secondKeyProject,
					keyType, comparison, isEWHD2Histogram, isEWHS1Histogram,
					conf);
		} else if (isOkcanSampling) {
			_queryBuilder = MyUtilities.addOkcanSampler(relationOrders1,
					relationOrders2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		} else if (isEWHSampling) {
			_queryBuilder = MyUtilities.addEWHSampler(relationOrders1,
					relationOrders2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		} else {
			final int Theta_JoinType = ThetaQueryPlansParameters
					.getThetaJoinType(conf);
			final ColumnReference colO1 = new ColumnReference(keyType,
					firstKeyProject);
			final ColumnReference colO2 = new ColumnReference(keyType,
					secondKeyProject);

			ComparisonPredicate pred5 = new ComparisonPredicate(
					ComparisonPredicate.NONGREATER_OP, colO1, colO2,
					comparisonValue, ComparisonPredicate.BPLUSTREE);

			// AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationOrders1,
							relationOrders2, _queryBuilder)
					.setJoinPredicate(pred5)
					.setContentSensitiveThetaJoinWrapper(keyType);
			// .addOperator(agg)
			// lastJoiner.setPrintOut(false);

			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY");
			_queryBuilder.add(dummy);
		}

	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
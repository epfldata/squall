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
import ch.epfl.data.squall.conversion.DateIntegerConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.ewh.components.DummyComponent;
import ch.epfl.data.squall.examples.imperative.theta.ThetaQueryPlansParameters;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;

//Bcb
public class ThetaEWHBandJPS {
	private static Logger LOG = Logger.getLogger(ThetaEWHBandJPS.class);

	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private DateIntegerConversion _dic = new DateIntegerConversion();

	public ThetaEWHBandJPS(String dataPath, String extension, Map conf) {
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "jps1_1";
		String matName2 = "jps2_2";
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

		Component relationJPS1, relationJPS2;
		// Full schema is
		// <id,peer_id,torrent_snapshot_id,upload_speed,download_speed,payload_upload_speed,payload_download_speed,total_upload,total_download,fail_count,hashfail_count,progress,created
		// total_upload is field 7, total_download is field 8

		ColumnReference col1 = new ColumnReference(_ic, 0);
		ColumnReference col2 = new ColumnReference(_ic, 1);
		ColumnReference col3 = new ColumnReference(_ic, 2);
		ColumnReference col4 = new ColumnReference(_ic, 3);

		ProjectOperator projectionJPS1 = new ProjectOperator(col1, col2, col3,
				col4);
		ProjectOperator projectionJPS2 = new ProjectOperator(col1, col2, col3,
				col4);
		// total_upload is field 2, total_download is field 3 in projectionPeer1
		// and projectionPeer2
		final List<Integer> hashJPS1 = Arrays.asList(3);
		final List<Integer> hashJPS2 = Arrays.asList(3);

		if (!isMaterialized) {
			// build relations
			relationJPS1 = new DataSourceComponent("JPS1", dataPath + "jps_1"
					+ extension).add(print1).add(projectionJPS1)
					.setOutputPartKey(hashJPS1);
			_queryBuilder.add(relationJPS1);

			relationJPS2 = new DataSourceComponent("JPS2", dataPath + "jps_2"
					+ extension).add(print2).add(projectionJPS2)
					.setOutputPartKey(hashJPS2);
			_queryBuilder.add(relationJPS2);
		} else {
			relationJPS1 = new DataSourceComponent("JPS1", dataPath + matName1
					+ extension).add(print1).add(projectionJPS1)
					.setOutputPartKey(hashJPS1);
			_queryBuilder.add(relationJPS1);

			relationJPS2 = new DataSourceComponent("JPS2", dataPath + matName2
					+ extension).add(print2).add(projectionJPS2)
					.setOutputPartKey(hashJPS2);
			_queryBuilder.add(relationJPS2);
		}

		NumericConversion keyType = _ic;
		int comparisonValue = 1; // default for this join
		if (SystemParameters.isExisting(conf, "COMPARISON_VALUE")) {
			comparisonValue = SystemParameters.getInt(conf, "COMPARISON_VALUE");
			LOG.info("ComparisonValue read from the config file: "
					+ comparisonValue);
		}
		ComparisonPredicate comparison = new ComparisonPredicate(
				ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue,
				keyType);
		int firstKeyProject = 3;
		int secondKeyProject = 3;

		if (printSelected) {
			relationJPS1.setPrintOut(false);
			relationJPS2.setPrintOut(false);
		} else if (isSrcHistogram) {
			_queryBuilder = MyUtilities.addSrcHistogram(relationJPS1,
					firstKeyProject, relationJPS2, secondKeyProject, keyType,
					comparison, isEWHD2Histogram, isEWHS1Histogram, conf);
		} else if (isOkcanSampling) {
			_queryBuilder = MyUtilities.addOkcanSampler(relationJPS1,
					relationJPS2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		} else if (isEWHSampling) {
			_queryBuilder = MyUtilities.addEWHSampler(relationJPS1,
					relationJPS2, firstKeyProject, secondKeyProject,
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
					.createThetaJoinOperator(Theta_JoinType, relationJPS1,
							relationJPS2, _queryBuilder)
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
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


package ch.epfl.data.squall.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.conversion.DateIntegerConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.ewh.components.DummyComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.theta.ThetaQueryPlansParameters;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class ThetaEWHBandPeer {
	private QueryBuilder _queryBuilder = new QueryBuilder();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private DateIntegerConversion _dic = new DateIntegerConversion();

	public ThetaEWHBandPeer(String dataPath, String extension, Map conf) {
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "peer1_1";
		String matName2 = "peer1_2";
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

		Component relationPeer1, relationPeer2;
		// Full schema is
		// <id,peer_id,torrent_snapshot_id,upload_speed,download_speed,payload_upload_speed,payload_download_speed,total_upload,total_download,fail_count,hashfail_count,progress,created
		// total_upload is field 7, total_download is field 8

		ColumnReference col1 = new ColumnReference(_ic, 0);
		ColumnReference col2 = new ColumnReference(_ic, 2);
		ColumnReference col3 = new ColumnReference(_ic, 7);
		ColumnReference col4 = new ColumnReference(_ic, 8);

		ProjectOperator projectionPeer1 = new ProjectOperator(col1, col2, col3,
				col4);
		ProjectOperator projectionPeer2 = new ProjectOperator(col1, col2, col3,
				col4);
		// total_upload is field 2, total_download is field 3 in projectionPeer1
		// and projectionPeer2
		final List<Integer> hashPeer1 = Arrays.asList(2);
		final List<Integer> hashPeer2 = Arrays.asList(3);

		if (!isMaterialized) {
			// eliminate inactive users
			// total_upload is field 7, total_download is field 8 in the base
			// relation
			ComparisonPredicate sel11 = new ComparisonPredicate(
					ComparisonPredicate.GREATER_OP,
					new ColumnReference(_ic, 7), new ValueSpecification(_ic, 0));
			SelectOperator selectionPeer1 = new SelectOperator(sel11);

			ComparisonPredicate sel12 = new ComparisonPredicate(
					ComparisonPredicate.GREATER_OP,
					new ColumnReference(_ic, 8), new ValueSpecification(_ic, 0));
			SelectOperator selectionPeer2 = new SelectOperator(sel12);

			// build relations
			relationPeer1 = new DataSourceComponent("PEER1", dataPath
					+ "peersnapshot-01" + extension).add(selectionPeer1)
					.add(print1).add(projectionPeer1)
					.setOutputPartKey(hashPeer1);
			_queryBuilder.add(relationPeer1);

			relationPeer2 = new DataSourceComponent("PEER2", dataPath
					+ "peersnapshot-01" + extension).add(selectionPeer2)
					.add(print2).add(projectionPeer2)
					.setOutputPartKey(hashPeer2);
			_queryBuilder.add(relationPeer2);
		} else {
			relationPeer1 = new DataSourceComponent("PEER1", dataPath
					+ matName1 + extension).add(projectionPeer1)
					.setOutputPartKey(hashPeer1);
			_queryBuilder.add(relationPeer1);

			relationPeer2 = new DataSourceComponent("PEER1", dataPath
					+ matName2 + extension).add(projectionPeer2)
					.setOutputPartKey(hashPeer2);
			_queryBuilder.add(relationPeer2);
		}

		NumericConversion keyType = _ic;
		int comparisonValue = 2;
		ComparisonPredicate comparison = new ComparisonPredicate(
				ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue,
				keyType);
		int firstKeyProject = 2;
		int secondKeyProject = 3;

		if (printSelected) {
			relationPeer1.setPrintOut(false);
			relationPeer2.setPrintOut(false);
		} else if (isOkcanSampling) {
			_queryBuilder = MyUtilities.addOkcanSampler(relationPeer1,
					relationPeer2, firstKeyProject, secondKeyProject,
					_queryBuilder, keyType, comparison, conf);
		} else if (isEWHSampling) {
			_queryBuilder = MyUtilities.addEWHSampler(relationPeer1,
					relationPeer2, firstKeyProject, secondKeyProject,
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
					.createThetaJoinOperator(Theta_JoinType, relationPeer1,
							relationPeer2, _queryBuilder)
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
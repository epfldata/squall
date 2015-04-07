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

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.ewh.components.EWHSampleMatrixComponent;
import ch.epfl.data.squall.ewh.data_structures.NumOfBuckets;
import ch.epfl.data.squall.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DateIntegerType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class EWHSampleMatrixPlan extends QueryPlan {
	private static Logger LOG = Logger.getLogger(EWHSampleMatrixPlan.class);
	private QueryBuilder _queryBuilder = new QueryBuilder();

	public EWHSampleMatrixPlan(String dataPath, String extension, Map conf) {
		// can be extracted from the complete query plan
		String firstCompName = SystemParameters.getString(conf,
				"FIRST_COMP_NAME");
		String secondCompName = SystemParameters.getString(conf,
				"SECOND_COMP_NAME");
		int firstProjection = SystemParameters
				.getInt(conf, "FIRST_KEY_PROJECT");
		int secondProjection = SystemParameters.getInt(conf,
				"SECOND_KEY_PROJECT");
		NumericType keyType;
		String keyTypeStr = SystemParameters.getString(conf, "KEY_TYPE_STR");
		if (keyTypeStr.equals("DATE_INTEGER")) {
			keyType = new DateIntegerType();
		} else if (keyTypeStr.equals("INTEGER")) {
			keyType = new IntegerType();
		} else {
			throw new RuntimeException("Unsupported type " + keyTypeStr);
		}
		ComparisonPredicate comparison;
		String comparisonStr = SystemParameters.getString(conf,
				"COMPARISON_TYPE");
		if (comparisonStr.equals("EQUAL")) {
			comparison = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP);
		} else if (comparisonStr.equals("SYM_BAND_WITH_BOUNDS_OP")) {
			int comparisonValue = SystemParameters.getInt(conf,
					"COMPARISON_VALUE");
			comparison = new ComparisonPredicate(
					ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP,
					comparisonValue, keyType);
		} else {
			throw new RuntimeException("Unsupported comparison "
					+ comparisonStr);
		}

		// cannot be extracted from the complete query plan
		String firstSrcFile = SystemParameters
				.getString(conf, "FIRST_SRC_FILE");
		String secondSrcFile = SystemParameters.getString(conf,
				"SECOND_SRC_FILE");
		int firstRelSize = SystemParameters.getInt(conf, "FIRST_REL_SIZE");
		int secondRelSize = SystemParameters.getInt(conf, "SECOND_REL_SIZE");
		long totalOutputSize = SystemParameters.getLong(conf,
				"TOTAL_OUTPUT_SIZE");
		int numLastJoiners = SystemParameters.getInt(conf, "PAR_LAST_JOINERS");

		ProjectOperator projectionLineitem1 = new ProjectOperator(
				new int[] { firstProjection });
		ProjectOperator projectionLineitem2 = new ProjectOperator(
				new int[] { secondProjection });
		final List<Integer> hash = Arrays.asList(0); // not important, always 0

		// compute Number of buckets
		NumOfBuckets numOfBuckets = MyUtilities.computeSampleMatrixBuckets(
				firstRelSize, secondRelSize, numLastJoiners, conf);
		int firstNumOfBuckets = numOfBuckets.getXNumOfBuckets();
		int secondNumOfBuckets = numOfBuckets.getYNumOfBuckets();
		LOG.info("In the sample matrix, FirstNumOfBuckets = "
				+ firstNumOfBuckets + ", SecondNumOfBuckets = "
				+ secondNumOfBuckets);

		DataSourceComponent relation1 = new DataSourceComponent(firstCompName,
				dataPath + firstSrcFile + extension).add(projectionLineitem1)
				.setOutputPartKey(hash);
		_queryBuilder.add(relation1);

		DataSourceComponent relation2 = new DataSourceComponent(secondCompName,
				dataPath + secondSrcFile + extension).add(projectionLineitem2)
				.setOutputPartKey(hash);
		_queryBuilder.add(relation2);

		// equi-weight histogram
		// send something to the extra partitioner node
		relation1.setPartitioner(true);
		relation2.setPartitioner(true);
		// add operators which samples for partitioner
		SampleAsideAndForwardOperator saf1 = new SampleAsideAndForwardOperator(
				firstRelSize, firstNumOfBuckets, SystemParameters.PARTITIONER,
				conf);
		SampleAsideAndForwardOperator saf2 = new SampleAsideAndForwardOperator(
				secondRelSize, secondNumOfBuckets,
				SystemParameters.PARTITIONER, conf);
		relation1.add(saf1);
		relation2.add(saf2);

		// do we build d2 out of the first relation (_firstParent)?
		boolean isFirstD2 = SystemParameters.getBoolean(conf, "IS_FIRST_D2");
		EWHSampleMatrixComponent ewhComp = new EWHSampleMatrixComponent(
				relation1, relation2, isFirstD2, keyType, comparison,
				numLastJoiners, firstRelSize, secondRelSize, firstNumOfBuckets,
				secondNumOfBuckets);
		_queryBuilder.add(ewhComp);
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}

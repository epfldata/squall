package ch.epfl.data.plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.conversion.DateIntegerConversion;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.ewh.components.OkcanSampleMatrixComponent;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SampleOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class OkcanSampleMatrixPlan {
	private QueryBuilder _queryBuilder = new QueryBuilder();

	public OkcanSampleMatrixPlan(String dataPath, String extension, Map conf) {
		// can be extracted from the complete query plan
		String firstCompName = SystemParameters.getString(conf,
				"FIRST_COMP_NAME");
		String secondCompName = SystemParameters.getString(conf,
				"SECOND_COMP_NAME");
		int firstProjection = SystemParameters
				.getInt(conf, "FIRST_KEY_PROJECT");
		int secondProjection = SystemParameters.getInt(conf,
				"SECOND_KEY_PROJECT");
		NumericConversion keyType;
		String keyTypeStr = SystemParameters.getString(conf, "KEY_TYPE_STR");
		if (keyTypeStr.equals("DATE_INTEGER")) {
			keyType = new DateIntegerConversion();
		} else if (keyTypeStr.equals("INTEGER")) {
			keyType = new IntegerConversion();
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
		int firstNumOfBuckets = SystemParameters.getInt(conf,
				"FIRST_NUM_OF_BUCKETS");
		int secondNumOfBuckets = SystemParameters.getInt(conf,
				"SECOND_NUM_OF_BUCKETS");
		int numLastJoiners = SystemParameters.getInt(conf, "PAR_LAST_JOINERS");

		ProjectOperator projectionLineitem1 = new ProjectOperator(
				new int[] { firstProjection });
		ProjectOperator projectionLineitem2 = new ProjectOperator(
				new int[] { secondProjection });
		final List<Integer> hash = Arrays.asList(0); // not important, always 0
		SampleOperator sample1 = new SampleOperator(firstRelSize,
				firstNumOfBuckets);
		SampleOperator sample2 = new SampleOperator(secondRelSize,
				secondNumOfBuckets);

		DataSourceComponent relation1 = new DataSourceComponent(firstCompName,
				dataPath + firstSrcFile + extension).add(projectionLineitem1)
				.add(sample1).setOutputPartKey(hash);
		_queryBuilder.add(relation1);

		DataSourceComponent relation2 = new DataSourceComponent(secondCompName,
				dataPath + secondSrcFile + extension).add(projectionLineitem2)
				.add(sample2).setOutputPartKey(hash);
		_queryBuilder.add(relation2);

		OkcanSampleMatrixComponent okcanComp = new OkcanSampleMatrixComponent(
				relation1, relation2, keyType, comparison, numLastJoiners,
				firstNumOfBuckets, secondNumOfBuckets);
		_queryBuilder.add(okcanComp);
	}

	public QueryBuilder getQueryPlan() {
		return _queryBuilder;
	}
}
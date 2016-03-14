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

package ch.epfl.data.squall.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import ch.epfl.data.squall.storm_components.hyper_cube.stream_grouping.HyperCubeGrouping;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignment;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponent;
import ch.epfl.data.squall.ewh.algorithms.DenseMonotonicWeightPrecomputation;
import ch.epfl.data.squall.ewh.algorithms.PWeightPrecomputation;
import ch.epfl.data.squall.ewh.algorithms.ShallowCoarsener;
import ch.epfl.data.squall.ewh.algorithms.WeightPrecomputation;
import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.components.EWHSampleMatrixComponent;
import ch.epfl.data.squall.ewh.components.EquiDepthHistogramComponent;
import ch.epfl.data.squall.ewh.components.OkcanSampleMatrixComponent;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.ListAdapter;
import ch.epfl.data.squall.ewh.data_structures.ListJavaGeneric;
import ch.epfl.data.squall.ewh.data_structures.ListTIntAdapter;
import ch.epfl.data.squall.ewh.data_structures.ListTLongAdapter;
import ch.epfl.data.squall.ewh.data_structures.NumOfBuckets;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterIntMatrix;
import ch.epfl.data.squall.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.squall.ewh.storm_components.stream_grouping.RangeFilteredMulticastStreamGrouping;
import ch.epfl.data.squall.ewh.storm_components.stream_grouping.RangeMulticastStreamGrouping;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.StormSrcHarmonizer;
import ch.epfl.data.squall.storm_components.hash_hypercube.HashHyperCubeGrouping;
import ch.epfl.data.squall.storm_components.hash_hypercube.HashHyperCubeGrouping.EmitterDesc;
import ch.epfl.data.squall.storm_components.stream_grouping.BatchStreamGrouping;
import ch.epfl.data.squall.storm_components.stream_grouping.HashStreamGrouping;
import ch.epfl.data.squall.storm_components.stream_grouping.ShuffleStreamGrouping;
import ch.epfl.data.squall.storm_components.theta.stream_grouping.ContentInsensitiveThetaJoinGrouping;
import ch.epfl.data.squall.storm_components.theta.stream_grouping.ContentSensitiveThetaJoinGrouping;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ContentSensitiveMatrixAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.MatrixAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignmentBruteForce.ColumnDesc;
import ch.epfl.data.squall.types.DateIntegerType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class MyUtilities {
    public static QueryBuilder addEWHSampler(Component firstParent,
	    Component secondParent, int firstKeyProject, int secondKeyProject,
	    QueryBuilder queryPlan, NumericType keyType,
	    ComparisonPredicate comparison, Map conf) {
	ProjectOperator project1 = new ProjectOperator(
		new int[] { firstKeyProject });
	ProjectOperator project2 = new ProjectOperator(
		new int[] { secondKeyProject });
	return addEWHSampler(firstParent, secondParent, project1, project2,
		queryPlan, keyType, comparison, conf);
    }

    public static QueryBuilder addEWHSampler(Component firstParent,
	    Component secondParent, ProjectOperator project1,
	    ProjectOperator project2, QueryBuilder queryBuilder,
	    NumericType keyType, ComparisonPredicate comparison, Map conf) {
	int firstRelSize = SystemParameters.getInt(conf, "FIRST_REL_SIZE");
	int secondRelSize = SystemParameters.getInt(conf, "SECOND_REL_SIZE");
	int numLastJoiners = SystemParameters.getInt(conf, "PAR_LAST_JOINERS");

	// compute Number of buckets
	NumOfBuckets numOfBuckets = MyUtilities.computeSampleMatrixBuckets(
		firstRelSize, secondRelSize, numLastJoiners, conf);
	int firstNumOfBuckets = numOfBuckets.getXNumOfBuckets();
	int secondNumOfBuckets = numOfBuckets.getYNumOfBuckets();
	LOG.info("In the sample matrix, FirstNumOfBuckets = "
		+ firstNumOfBuckets + ", SecondNumOfBuckets = "
		+ secondNumOfBuckets);

	// hash is always 0 as the key is there
	List<Integer> hash = new ArrayList<Integer>(Arrays.asList(0));

	firstParent.add(project1).setOutputPartKey(hash);
	secondParent.add(project2).setOutputPartKey(hash);

	// equi-weight histogram
	// send something to the extra partitioner node
	setParentPartitioner(firstParent);
	setParentPartitioner(secondParent);
	// add operators which samples for partitioner
	SampleAsideAndForwardOperator saf1 = new SampleAsideAndForwardOperator(
		firstRelSize, firstNumOfBuckets, SystemParameters.PARTITIONER,
		conf);
	SampleAsideAndForwardOperator saf2 = new SampleAsideAndForwardOperator(
		secondRelSize, secondNumOfBuckets,
		SystemParameters.PARTITIONER, conf);
	firstParent.add(saf1);
	secondParent.add(saf2);

	// do we build d2 out of the first relation (_firstParent)?
	boolean isFirstD2 = SystemParameters.getBoolean(conf, "IS_FIRST_D2");
	EWHSampleMatrixComponent ewhComp = new EWHSampleMatrixComponent(
		firstParent, secondParent, isFirstD2, keyType, comparison,
		numLastJoiners, firstRelSize, secondRelSize, firstNumOfBuckets,
		secondNumOfBuckets);
	queryBuilder.add(ewhComp);
	return queryBuilder;
    }

    public static QueryBuilder addOkcanSampler(Component firstParent,
	    Component secondParent, int firstKeyProject, int secondKeyProject,
	    QueryBuilder queryPlan, NumericType keyType,
	    ComparisonPredicate comparison, Map conf) {
	ProjectOperator project1 = new ProjectOperator(
		new int[] { firstKeyProject });
	ProjectOperator project2 = new ProjectOperator(
		new int[] { secondKeyProject });

	return addOkcanSampler(firstParent, secondParent, project1, project2,
		queryPlan, keyType, comparison, conf);
    }

    public static QueryBuilder addOkcanSampler(Component firstParent,
	    Component secondParent, ProjectOperator project1,
	    ProjectOperator project2, QueryBuilder queryBuilder,
	    NumericType keyType, ComparisonPredicate comparison, Map conf) {
	int firstRelSize = SystemParameters.getInt(conf, "FIRST_REL_SIZE");
	int secondRelSize = SystemParameters.getInt(conf, "SECOND_REL_SIZE");
	int firstNumOfBuckets = SystemParameters.getInt(conf,
		"FIRST_NUM_OF_BUCKETS");
	int secondNumOfBuckets = SystemParameters.getInt(conf,
		"SECOND_NUM_OF_BUCKETS");
	int numLastJoiners = SystemParameters.getInt(conf, "PAR_LAST_JOINERS");

	// adjust the number of buckets such that no bucket is more than 2x
	// bigger than others
	firstNumOfBuckets = MyUtilities.adjustPartitioning(firstRelSize,
		firstNumOfBuckets, "c_x");
	secondNumOfBuckets = MyUtilities.adjustPartitioning(secondRelSize,
		secondNumOfBuckets, "c_y");

	// hash is always 0 as the key is there
	List<Integer> hash = new ArrayList<Integer>(Arrays.asList(0));

	SampleOperator sample1 = new SampleOperator(firstRelSize,
		firstNumOfBuckets);
	SampleOperator sample2 = new SampleOperator(secondRelSize,
		secondNumOfBuckets);
	firstParent.add(sample1).add(project1).setOutputPartKey(hash);
	secondParent.add(sample2).add(project2).setOutputPartKey(hash);

	// In principle, we could run this on non-materialized relations as well
	// Instead of JoinComponent, we just put OkcanSampleMatrixComponent
	OkcanSampleMatrixComponent okcanComp = new OkcanSampleMatrixComponent(
		firstParent, secondParent, keyType, comparison, numLastJoiners,
		firstNumOfBuckets, secondNumOfBuckets);
	queryBuilder.add(okcanComp);

	return queryBuilder;
    }

    private static void addSampleOp(Component parent, ProjectOperator project,
	    int relSize, int numLastJoiners, Map conf) {
	// hash is always 0 as the key is there
	List<Integer> hash = new ArrayList<Integer>(Arrays.asList(0));

	SampleOperator sample = new SampleOperator(relSize, numLastJoiners);
	parent.add(sample).add(project).setOutputPartKey(hash);
    }

    public static QueryBuilder addSrcHistogram(Component relationJPS1,
	    int firstKeyProject, Component relationJPS2, int secondKeyProject,
	    NumericType keyType, ComparisonPredicate comparison,
	    boolean isEWHD2Histogram, boolean isEWHS1Histogram, Map conf) {
	QueryBuilder queryBuilder = new QueryBuilder();
	int relSize1 = -1, relSize2 = -1;
	int keyProject1 = -1, keyProject2 = -1;
	Component r1 = null, r2 = null; // r2 feeds D2Combiner, r1 feeds
					// S1Reservoir directly

	boolean isFirstD2 = SystemParameters.getBoolean(conf, "IS_FIRST_D2");
	if (isFirstD2) {
	    relSize2 = SystemParameters.getInt(conf, "FIRST_REL_SIZE");
	    r2 = relationJPS1;
	    keyProject2 = firstKeyProject;

	    relSize1 = SystemParameters.getInt(conf, "SECOND_REL_SIZE");
	    r1 = relationJPS2;
	    keyProject1 = secondKeyProject;
	} else {
	    relSize1 = SystemParameters.getInt(conf, "FIRST_REL_SIZE");
	    r1 = relationJPS1;
	    keyProject1 = firstKeyProject;

	    relSize2 = SystemParameters.getInt(conf, "SECOND_REL_SIZE");
	    r2 = relationJPS2;
	    keyProject2 = secondKeyProject;
	}

	ProjectOperator project1 = new ProjectOperator(
		new int[] { keyProject1 });
	ProjectOperator project2 = new ProjectOperator(
		new int[] { keyProject2 });

	// one or two relations are part of the query plan +
	// createHistogramComponent
	int numLastJoiners = SystemParameters.getInt(conf, "PAR_LAST_JOINERS");
	if (isEWHD2Histogram) {
	    addSampleOp(r2, project2, relSize2, numLastJoiners, conf);
	    queryBuilder.add(r2);
	} else {
	    r2 = null;
	}
	if (isEWHS1Histogram) {
	    addSampleOp(r1, project1, relSize1, numLastJoiners, conf);
	    queryBuilder.add(r1);
	} else {
	    r1 = null;
	}
	EquiDepthHistogramComponent r2HistComp = new EquiDepthHistogramComponent(
		r1, r2, keyType, comparison, numLastJoiners);
	queryBuilder.add(r2HistComp);

	return queryBuilder;
    }

    /*
     * Necessary to avoid the last bucket to be very large in
     * EWHSampleMatrixBolt.createBoundaries For example, if relSize = 12000 and
     * numOfBuckets = 6001, EWHSampleMatrixBolt.createBoundaries creates 6000
     * buckets of size 1 and 1 bucket of size 6000 The last bucket is 6000X
     * bigger than any previous bucket Here, we ensure that the last bucket is
     * at most 2X larger than any previous bucket
     * 
     * numOfBuckets = n_s
     * 
     * This method can also be invoked for (n_s, p)
     */
    public static int adjustPartitioning(int relSize, int numOfBuckets,
	    String parameterName) {
	if (relSize % numOfBuckets != 0) {
	    // roof of bucketSize: increasing bucketSize reduces numOfBuckets
	    int bucketSize = (relSize / numOfBuckets) + 1;
	    if (numOfBuckets >= bucketSize) {
		// Meant for the case numOfBuckets >= bucketSize.
		// If that's not the case, no need for any adjustment.

		/*
		 * relSize and bucketSize are integers; newBucketSize is double
		 * of form x.y We have x buckets of size bucketSize; the last
		 * bucket has 0.y * bucketSize, and this is at most bucketSize
		 * Hence, the last bucket is at most two times larger than the
		 * previous buckets
		 */
		int newNumOfBuckets = relSize / bucketSize;

		LOG.info("Reducing the size of " + parameterName + " from "
			+ numOfBuckets + " to " + newNumOfBuckets);
		/*
		 * if(numOfBuckets > 1.15 * newNumOfBuckets){ throw new
		 * RuntimeException(
		 * "No need to fix the code: just keep in mind that you reduced the number of buckets for more than 15%"
		 * ); }
		 */

		numOfBuckets = newNumOfBuckets;
	    }
	}
	return numOfBuckets;
    }

    public static InputDeclarer attachEmitterBatch(Map map,
	    List<String> fullHashList, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID,
			new BatchStreamGrouping(map, fullHashList));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterBroadcast(String streamId,
	    InputDeclarer currentBolt, String emitterId1,
	    String... emitterIdArray) {
	final List<String> emittersIdsList = new ArrayList<String>();
	emittersIdsList.add(emitterId1);
	emittersIdsList.addAll(Arrays.asList(emitterIdArray));

	for (final String emitterId : emittersIdsList) {
	    currentBolt = currentBolt.allGrouping(emitterId, streamId);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterBroadcast(InputDeclarer currentBolt, List<StormEmitter> emitters) {
        for (StormEmitter e : emitters) {
            for (String emitterId : e.getEmitterIDs()) {
                currentBolt = currentBolt.allGrouping(emitterId);
            }
        }
        return currentBolt;
    }

    public static InputDeclarer attachEmitterComponentsReshuffled(
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.shuffleGrouping(emitterID);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterFilteredRangeMulticast(
	    String streamId, Map map, ComparisonPredicate comparison,
	    NumericType wrapper, HistogramType dstHistType,
	    HistogramType srcHistType, String parentCompName,
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID, streamId,
			new RangeFilteredMulticastStreamGrouping(map,
				comparison, wrapper, dstHistType, srcHistType,
				parentCompName));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterHash(Map map,
	    List<String> fullHashList, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID,
			new HashStreamGrouping(map, fullHashList));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterHash(String streamId, Map map,
	    List<String> fullHashList, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID, streamId,
			new HashStreamGrouping(map, fullHashList));
	}
	return currentBolt;
    }

    // TODO the following two methods can be shortened by invoking each other
    public static InputDeclarer attachEmitterRange(Map map,
	    NumericType wrapper, HistogramType histType,
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt
			.customGrouping(emitterID,
				new RangeMulticastStreamGrouping(map, wrapper,
					histType));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterRange(Map map,
	    NumericType wrapper, HistogramType histType,
	    InputDeclarer currentBolt, String emitterId1,
	    String... emitterIdArray) {
	final List<String> emittersIdsList = new ArrayList<String>();
	emittersIdsList.add(emitterId1);
	emittersIdsList.addAll(Arrays.asList(emitterIdArray));

	for (final String emitterId : emittersIdsList) {
	    currentBolt = currentBolt.customGrouping(emitterId,
		    new RangeMulticastStreamGrouping(map, wrapper, histType));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterRangeMulticast(Map map,
	    ComparisonPredicate comparison, NumericType wrapper,
	    HistogramType histType, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID,
			new RangeMulticastStreamGrouping(map, comparison,
				wrapper, histType));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterRangeMulticast(String streamId,
	    Map map, ComparisonPredicate comparison, NumericType wrapper,
	    HistogramType histType, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID, streamId,
			new RangeMulticastStreamGrouping(map, comparison,
				wrapper, histType));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterShuffle(Map map,
	    InputDeclarer currentBolt, String emitterId1,
	    String... emitterIdArray) {
	final List<String> emittersIdsList = new ArrayList<String>();
	emittersIdsList.add(emitterId1);
	emittersIdsList.addAll(Arrays.asList(emitterIdArray));

	for (final String emitterId : emittersIdsList) {
	    currentBolt = currentBolt.customGrouping(emitterId,
		    new ShuffleStreamGrouping(map));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterToSingle(
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.globalGrouping(emitterID);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterToSingle(String streamId,
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.globalGrouping(emitterID, streamId);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterStarSchema(Map map,
        InputDeclarer currentBolt, List<StormEmitter> emitters,
        long[] cardinality) {
        String starEmitterName = emitters.get(0).getName();
        long largestCardinality = cardinality[0];
        // find the starEmitter as the one with largest cardinality
        for (int i = 1; i < emitters.size(); i++) {
            if (cardinality[i] > largestCardinality) {
                largestCardinality = cardinality[i];
                starEmitterName = emitters.get(i).getName();
            }
        }

        for (final StormEmitter emitter : emitters) {
            final String[] emitterIDs = emitter.getEmitterIDs();
            if (emitter.getName().equals(starEmitterName)) {
                for (final String emitterID : emitterIDs)
                    currentBolt = currentBolt.customGrouping(emitterID, new ShuffleStreamGrouping(map));
            } else {
                for (final String emitterID : emitterIDs) {
                    currentBolt = currentBolt.allGrouping(emitterID);
                }
            }
        }

        return currentBolt;
    }

    public static InputDeclarer attachEmitterHashHyperCube(
    		InputDeclarer currentBolt, List<StormEmitter> emitters, 
    		Map<String, String[]> emitterColNames, HashHyperCubeAssignment assignment, 
    		List<EmitterDesc> emittersDesc, Map map) {

        CustomStreamGrouping mapping = new HashHyperCubeGrouping(emittersDesc, assignment, map);

        for (final StormEmitter emitter : emitters) {
            final String[] emitterIDs = emitter.getEmitterIDs();
            for (final String emitterID : emitterIDs)
                currentBolt = currentBolt.customGrouping(emitterID, mapping);
        }
        return currentBolt;
    }

    public static List<EmitterDesc> getEmitterDesc(List<StormEmitter> emitters, 
    		Map<String, String[]> emitterColNames, long[] cardinality) {
    	List<EmitterDesc> emittersDesc = new ArrayList<EmitterDesc>();
        
        for (int i = 0; i < emitters.size(); i++) {
        	String emitterName = emitters.get(i).getName();
         	EmitterDesc ed = new EmitterDesc(emitterName, cardinality[i], emitterColNames.get(emitterName));
        	emittersDesc.add(ed);       	
        }

        return emittersDesc;
    }

    public static InputDeclarer attachEmitterHyperCube(
            InputDeclarer currentBolt, List<StormEmitter> emitters, List<String> allCompNames,
            HyperCubeAssignment assignment, Map map) {


        String[] emitterIndexes = new String[emitters.size()];
        for (int i = 0; i < emitterIndexes.length; i++)
            emitterIndexes[i] = String.valueOf(allCompNames
                    .indexOf(emitters.get(i).getName()));

        CustomStreamGrouping mapping = new HyperCubeGrouping(emitterIndexes, assignment, map);

        for (final StormEmitter emitter : emitters) {
            final String[] emitterIDs = emitter.getEmitterIDs();
            for (final String emitterID : emitterIDs)
                currentBolt = currentBolt.customGrouping(emitterID, mapping);
        }
        return currentBolt;
    }

    public static void checkBatchOutput(long batchOutputMillis,
	    AggregateOperator aggregation, Map conf) {
	if (aggregation == null && batchOutputMillis != 0)
	    throw new RuntimeException(
		    "A component must have aggregation operator in order to support batching.");
	if (isAckEveryTuple(conf) && batchOutputMillis != 0)
	    throw new RuntimeException(
		    "With batching, only AckAtEnd mode is allowed!");
	// we don't keep Storm Tuple instances for batched tuples
	// we also ack them immediately, which doesn't fir in AckEveryTime
	// logic
    }

    public static void checkEquality(WeightPrecomputation first,
	    PWeightPrecomputation second) {
	int xSize = first.getXSize();
	int ySize = first.getYSize();
	if (xSize != second.getXSize() || ySize != second.getYSize()) {
	    throw new RuntimeException(
		    "The two WeightPrecomputation has different sizes: ("
			    + xSize + ", " + ySize + ") " + "versus ("
			    + second.getXSize() + ", " + second.getYSize()
			    + ")");
	}

	for (int i = 0; i < xSize; i++) {
	    for (int j = 0; j < ySize; j++) {
		int fps = first.getPrefixSum(i, j);
		int sps = second.getPrefixSum(i, j);
		if (fps != sps) {
		    throw new RuntimeException(
			    "The two WeightPrecomputation differ at position ("
				    + i + ", " + j + "): " + fps + " and "
				    + sps + ".");
		}
	    }
	}

	LOG.info("The two WeightPrecomputation are the same!");
    }

    public static void checkIIPrecValid(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	List<String> values = new ArrayList<String>(Arrays.asList("DENSE",
		"PWEIGHT", "BOTH"));
	if (SystemParameters.isExisting(conf, key)) {
	    String value = SystemParameters.getString(conf, key);
	    if (!values.contains(value)) {
		throw new RuntimeException(
			"Unsupported value for SECOND_PRECOMPUTATION = "
				+ value);
	    }
	}
    }

    public static boolean checkSendMode(Map map) {
	if (SystemParameters.isExisting(map, "BATCH_SEND_MODE")) {
	    final String mode = SystemParameters.getString(map,
		    "BATCH_SEND_MODE");
	    if (!mode.equalsIgnoreCase("THROTTLING")
		    && !mode.equalsIgnoreCase("SEND_AND_WAIT")
		    && !mode.equalsIgnoreCase("MANUAL_BATCH"))
		return false;
	}
	return true;
    }

    public static int chooseBalancedTargetIndex(String hash,
	    List<String> allHashes, int targetParallelism) {
	return allHashes.indexOf(hash) % targetParallelism;
    }

    public static int chooseHashTargetIndex(String hash, int targetParallelism) {
	return Math.abs(hash.hashCode()) % targetParallelism;
    }

    public static void compareActualAndSampleJM(JoinMatrix sampleMatrix,
	    ShallowCoarsener sampleSC, WeightPrecomputation sampleWP) {
	long start = System.currentTimeMillis();

	Map map = sampleMatrix.getConfiguration();
	WeightFunction wf = sampleWP.getWeightFunction();

	// initial check-ups
	int firstRelSize = SystemParameters.getInt(map, "FIRST_REL_SIZE");
	int secondRelSize = SystemParameters.getInt(map, "SECOND_REL_SIZE");
	int nsx = sampleMatrix.getXSize();
	int nsy = sampleMatrix.getYSize();
	if ((firstRelSize != nsx) || (secondRelSize != nsy)) {
	    throw new RuntimeException("Sample matrix " + nsx + "x" + nsy
		    + " must be of relation size " + firstRelSize + "x"
		    + secondRelSize + '!');
	}

	// let's create actual join matrix and its precomputation
	JoinMatrix actualMatrix = createAndFillActualMatrix(sampleMatrix);
	DenseMonotonicWeightPrecomputation actualWP = new DenseMonotonicWeightPrecomputation(
		wf, actualMatrix, map);

	// maxDiff parameters
	Region mdRegion = null;
	double mdSampleWeight = -1;
	double mdActualWeight = -1;
	double mdErrorPercent = -1;

	List<Region> originalRegions = sampleSC.getCandidateRoundedCells(map);
	for (Region originalRegion : originalRegions) {
	    // no matter of the sampleWP type, getWeight expects originalRegion
	    double sampleWeight = sampleWP.getWeight(originalRegion);
	    double actualWeight = actualWP.getWeight(originalRegion);
	    double errorPercent = computeDiffPercent(sampleWeight, actualWeight);
	    if (errorPercent > mdErrorPercent) {
		// max error so far: will be saved
		mdRegion = originalRegion;
		mdSampleWeight = sampleWeight;
		mdActualWeight = actualWeight;
		mdErrorPercent = errorPercent;
	    }

	    LOG.info("For candidate rounded cell (in terms of original matrix): "
		    + originalRegion);
	    LOG.info("   ActualWeight = " + actualWeight);
	    LOG.info("   SampleWeight = " + sampleWeight);
	}

	LOG.info("Maximum error is " + mdErrorPercent + "% for " + mdRegion);
	LOG.info("   Maximum ActualWeight = " + mdActualWeight);
	LOG.info("   Maximum SampleWeight = " + mdSampleWeight);

	double elapsed = (System.currentTimeMillis() - start) / 1000.0;
	LOG.info("Checking the precision of sampling takes " + elapsed
		+ " seconds.");
    }

    private static double computeDiffPercent(double first, double second) {
	double bigger = first > second ? first : second;
	double smaller = first < second ? first : second;
	return (bigger / smaller - 1) * 100;
    }

    public static int computeManualSampleSize(Config conf,
	    int firstNumOfBuckets, int secondNumOfBuckets) {
	String mode = SystemParameters.getString(conf,
		"OUTPUT_SAMPLE_SIZE_MODE");
	int constant = SystemParameters.getInt(conf,
		"OUTPUT_SAMPLE_SIZE_CONSTANT");
	if (mode.equalsIgnoreCase("MULTIPLY")) {
	    int n_c_s = MyUtilities.getMax(firstNumOfBuckets,
		    secondNumOfBuckets);
	    return constant * n_c_s; // the actual output sample size is
				     // currently upper bounded by the size
				     // of the bigger relation
	} else if (mode.equalsIgnoreCase("EXACT")) {
	    return constant;
	} else {
	    throw new RuntimeException("Unsupported OUTPUT_SAMPLE_SIZE_MODE "
		    + mode);
	}
    }

    public static double computePercentage(int smaller, int bigger) {
	int diff = bigger - smaller; // always positive
	return ((double) diff) / bigger;
    }

    public static NumOfBuckets computeSampleMatrixBuckets(int firstRelSize,
	    int secondRelSize, int numLastJoiners, Map conf) {
	int firstNumOfBuckets, secondNumOfBuckets;
	/*
	 * if(!SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED")){ // the
	 * same as relation sizes firstNumOfBuckets = firstRelSize;
	 * secondNumOfBuckets = secondRelSize; }else
	 */if (SystemParameters.isExisting(conf, "FIRST_NUM_OF_BUCKETS")
		&& SystemParameters.isExisting(conf, "SECOND_NUM_OF_BUCKETS")) {
	    // manually
	    firstNumOfBuckets = SystemParameters.getInt(conf,
		    "FIRST_NUM_OF_BUCKETS");
	    secondNumOfBuckets = SystemParameters.getInt(conf,
		    "SECOND_NUM_OF_BUCKETS");
	} else {
	    // automatically
	    int overprovision = 2;
	    long m = getM(firstRelSize, secondRelSize, conf);
	    String bucketsType = SystemParameters.getString(conf,
		    "SAMPLE_MATRIX_BUCKET_TYPE");
	    if (bucketsType.equals("EQUI_BUCKET_SIZE")) {
		// (firstRelSize/firstNumBuckets) *
		// (secondRelSize/secondNumBuckets) <= n/2J
		// bucketSize = firstRelSize/firstNumBuckets =
		// secondRelSize/secondNumBuckets
		// overprovision = 2
		int bucketSize = (int) Math.sqrt(((double) m)
			/ (overprovision * numLastJoiners));
		firstNumOfBuckets = firstRelSize / bucketSize;
		secondNumOfBuckets = secondRelSize / bucketSize;
	    } else if (bucketsType.equals("EQUI_BUCKET_NUMBER")) {
		// (firstRelSize/firstNumBuckets) *
		// (secondRelSize/secondNumBuckets) <= n/2J
		// firstNumBuckets = secondNumBuckets = numBuckets
		// overprovision = 2
		int numBuckets = (int) Math.sqrt(((double) firstRelSize)
			* secondRelSize * overprovision * numLastJoiners / m);
		firstNumOfBuckets = numBuckets;
		secondNumOfBuckets = numBuckets;
	    } else {
		throw new RuntimeException("Unsupported bucket type "
			+ bucketsType);
	    }
	    // only in automatic mode, otherwise a user knows what he is doing
	    firstNumOfBuckets = MyUtilities.adjustPartitioning(firstRelSize,
		    firstNumOfBuckets, "n_s_x");
	    secondNumOfBuckets = MyUtilities.adjustPartitioning(secondRelSize,
		    secondNumOfBuckets, "n_s_y");
	}
	if (firstNumOfBuckets == 0 || secondNumOfBuckets == 0) {
	    throw new RuntimeException(
		    "Zero number of buckets! firstNumOfBuckets = "
			    + firstNumOfBuckets + ", secondNumOfBuckets = "
			    + secondNumOfBuckets);
	}
	return new NumOfBuckets(firstNumOfBuckets, secondNumOfBuckets);
    }

    private static JoinMatrix createAndFillActualMatrix(JoinMatrix sampleMatrix) {
	int xSize = sampleMatrix.getXSize();
	int ySize = sampleMatrix.getYSize();
	ComparisonPredicate cp = sampleMatrix.getComparisonPredicate();
	NumericType wrapper = sampleMatrix.getWrapper();
	Map conf = sampleMatrix.getConfiguration();

	// create matrix and fill it with the joinAttributes (keys)
	JoinMatrix actualMatrix = new UJMPAdapterIntMatrix(xSize, ySize, conf,
		cp, wrapper);
	for (int i = 0; i < xSize; i++) {
	    // sample matrix and actual matrix are of the same size
	    actualMatrix.setJoinAttributeX(sampleMatrix.getJoinAttributeX(i));
	}
	for (int i = 0; i < ySize; i++) {
	    actualMatrix.setJoinAttributeY(sampleMatrix.getJoinAttributeY(i));
	}

	// fill output
	int firstCandInLastLine = 0;
	for (int i = 0; i < xSize; i++) {
	    boolean isFirstInLine = true;
	    int x1 = i;
	    int x2 = i;
	    for (int j = firstCandInLastLine; j < ySize; j++) {
		int y1 = j;
		int y2 = j;
		// LOG.info("x1 = " + x1 + ", y1 = " + y1 + ", x2 = " + x2 +
		// ", y2 = " + y2);
		Region region = new Region(x1, y1, x2, y2);
		boolean isCandidate = MyUtilities.isCandidateRegion(
			actualMatrix, region, cp, conf);
		if (isCandidate) {
		    // previously it was firstKeys.get(i), secondKeys.get(j)
		    // corresponds to x/yBoundaries on n_s cells
		    if (cp.test(actualMatrix.getJoinAttributeX(i),
			    actualMatrix.getJoinAttributeY(j))) {
			actualMatrix.setElement(1, i, j);
		    }
		    if (isFirstInLine) {
			firstCandInLastLine = j;
			isFirstInLine = false;
		    }
		}
		if (!isFirstInLine && !isCandidate) {
		    // I am right from the candidate are; the first
		    // non-candidate guy means I should switch to the next row
		    break;
		}
	    }
	}

	return actualMatrix;
    }

    public static String createHashString(List<String> tuple,
	    List<Integer> hashIndexes, List<ValueExpression> hashExpressions,
	    Map map) {
	if (hashIndexes == null && hashExpressions == null)
	    return SINGLE_HASH_KEY;

	final String columnDelimiter = getColumnDelimiter(map);

	// NOTE THAT THE HASHCOLUMN depend upon the output of the projection!!
	final StringBuilder hashStrBuf = new StringBuilder();
	if (hashIndexes != null)
	    for (final int hashIndex : hashIndexes)
		hashStrBuf.append(tuple.get(hashIndex)).append(columnDelimiter);
	if (hashExpressions != null)
	    for (final ValueExpression hashExpression : hashExpressions)
		hashStrBuf.append(hashExpression.eval(tuple)).append(
			columnDelimiter);

	// remove one extra HASH_DELIMITER at the end

	final int hdLength = columnDelimiter.length();
	final int fullLength = hashStrBuf.length();
	return hashStrBuf.substring(0, fullLength - hdLength);

    }

    public static String createHashString(List<String> tuple,
	    List<Integer> hashIndexes, Map map) {
	if (hashIndexes == null || hashIndexes.isEmpty())
	    return SINGLE_HASH_KEY;
	String hashString = "";
	final int tupleLength = hashIndexes.size();
	for (int i = 0; i < tupleLength; i++)
	    // depend upon the output of the
	    // projection!!
	    if (i == tupleLength - 1)
		hashString += tuple.get(hashIndexes.get(i));
	    else
		hashString += tuple.get(hashIndexes.get(i))
			+ getColumnDelimiter(map);
	return hashString;
    }

    /*
     * For each emitter component (there are two input emitters for each join),
     * appropriately connect with all of its inner Components that emits tuples
     * to StormDestinationJoin. For destinationJoiner, there is only one bolt
     * that emits tuples, but for sourceJoiner, there are two SourceStorage (one
     * for storing each emitter tuples), which emits tuples.
     */
    /*
     * public static InputDeclarer attachEmitterComponents(InputDeclarer
     * currentBolt, StormEmitter emitter1, StormEmitter... emittersArray){
     * List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
     * emittersList.add(emitter1);
     * emittersList.addAll(Arrays.asList(emittersArray)); for(StormEmitter
     * emitter: emittersList){ String[] emitterIDs = emitter.getEmitterIDs();
     * for(String emitterID: emitterIDs){ currentBolt =
     * currentBolt.fieldsGrouping(emitterID, new Fields("Hash")); } } return
     * currentBolt; }
     */

    public static <T extends Comparable<T>> ListAdapter<T> createListAdapter(
	    Map conf) {
	String collectionAdapter = SystemParameters.getString(conf,
		"COLLECTION_ADAPTER");
	String keyTypeStr = SystemParameters.getString(conf, "KEY_TYPE_STR");

	if (collectionAdapter.equalsIgnoreCase("TROVE")) {
	    if (keyTypeStr.equalsIgnoreCase("INTEGER")) {
		return new ListTIntAdapter<T>();
	    } else if (keyTypeStr.equalsIgnoreCase("LONG")) {
		return new ListTLongAdapter<T>();
	    }
	} else if (collectionAdapter.equalsIgnoreCase("JAVA")) {
	    return new ListJavaGeneric<T>();
	}
	throw new RuntimeException("Unsupported type collectionAdapter = "
		+ collectionAdapter + ", keyTypeStr = " + keyTypeStr);
    }

    public static List<String> createOutputTuple(List<String> firstTuple,
	    List<String> secondTuple) {
	final List<String> outputTuple = new ArrayList<String>();

	for (int j = 0; j < firstTuple.size(); j++)
	    // first relation (R)
	    outputTuple.add(firstTuple.get(j));
	for (int j = 0; j < secondTuple.size(); j++)
	    outputTuple.add(secondTuple.get(j));
	return outputTuple;
    }

    public static List<String> createOutputTuple(List<String> firstTuple,
	    List<String> secondTuple, List<Integer> joinParams) {
	final List<String> outputTuple = new ArrayList<String>();

	for (int j = 0; j < firstTuple.size(); j++)
	    // first relation (R)
	    outputTuple.add(firstTuple.get(j));
	for (int j = 0; j < secondTuple.size(); j++)
	    if ((joinParams == null) || (!joinParams.contains(j)))
		// not
		// exits
		// add
		// the
		// column!!
		// (S)
		outputTuple.add(secondTuple.get(j));
	return outputTuple;
    }

    public static List<String> createOutputTuple(List<List<String>> tuples) {
        final List<String> outputTuple = new ArrayList<String>();

        for (List<String> tpI : tuples) {
            for (String coulumnJ : tpI) {
                outputTuple.add(coulumnJ);
            }
        }

        return outputTuple;
    }

    public static Values createRelSizeTuple(String componentIndex, int relSize) {
	Values relSizeTuple = new Values();
	relSizeTuple.add(componentIndex);
	List<String> tuple = new ArrayList<String>(Arrays.asList(
		SystemParameters.REL_SIZE, String.valueOf(relSize)));
	relSizeTuple.add(tuple);
	relSizeTuple.add("O"); // does not matter as we send to a single
			       // Partitioner node
	return relSizeTuple;
    }

    public static Values createTotalOutputSizeTuple(String componentIndex,
	    long totalOutputSize) {
	Values totalOutputSizeTuple = new Values();
	totalOutputSizeTuple.add(componentIndex);
	List<String> tuple = new ArrayList<String>(Arrays.asList(
		SystemParameters.TOTAL_OUTPUT_SIZE,
		String.valueOf(totalOutputSize)));
	totalOutputSizeTuple.add(tuple);
	totalOutputSizeTuple.add("O"); // does not matter as we send to a single
				       // Partitioner node
	return totalOutputSizeTuple;
    }

    public static Values createTupleValues(List<String> tuple, long timestamp,
	    String componentIndex, List<Integer> hashIndexes,
	    List<ValueExpression> hashExpressions, Map conf) {

	final String outputTupleHash = MyUtilities.createHashString(tuple,
		hashIndexes, hashExpressions, conf);
	if (MyUtilities.isCustomTimestampMode(conf)
		|| MyUtilities.isWindowTimestampMode(conf))
	    return new Values(componentIndex, tuple, outputTupleHash, timestamp);
	else
	    return new Values(componentIndex, tuple, outputTupleHash);
    }

    public static Values createUniversalFinalAckTuple(Map map) {
	final Values values = new Values();
	values.add("N/A");
	if (!MyUtilities.isManualBatchingMode(map)) {
	    final List<String> lastTuple = new ArrayList<String>(
		    Arrays.asList(SystemParameters.LAST_ACK));
	    values.add(lastTuple);
	    values.add("N/A");
	} else
	    values.add(SystemParameters.LAST_ACK);
	if (MyUtilities.isCustomTimestampMode(map)
		|| MyUtilities.isWindowTimestampMode(map))
	    values.add(0);
	return values;
    }

    public static void dumpSignal(StormComponent comp, Tuple stormTupleRcv,
	    OutputCollector collector) {
	comp.printContent();
	collector.ack(stormTupleRcv);
    }

    private static String extractSizeSkew(String dataPath) {
	String parts[] = dataPath.split("/");
	int size = parts.length;
	if (size == 1) {
	    return parts[0];
	} else {
	    return parts[size - 2] + "_" + parts[size - 1];
	}
    }

    /*
     * Different tuple<->(String, Hash) conversions
     */
    public static List<String> fileLineToTuple(String line, Map conf) {
	final String[] columnValues = line.split(SystemParameters.getString(
		conf, "DIP_READ_SPLIT_DELIMITER"));
	return new ArrayList<String>(Arrays.asList(columnValues));
    }

    // collects all the task ids for "default" stream id
    public static List<Integer> findTargetTaskIds(TopologyContext tc) {
	final List<Integer> result = new ArrayList<Integer>();
	final Map<String, Map<String, Grouping>> streamComponentGroup = tc
		.getThisTargets();
	final Iterator<Entry<String, Map<String, Grouping>>> it = streamComponentGroup
		.entrySet().iterator();
	while (it.hasNext()) {
	    final Map.Entry<String, Map<String, Grouping>> pair = it.next();
	    final String streamId = pair.getKey();
	    final Map<String, Grouping> componentGroup = pair.getValue();
	    if (streamId.equalsIgnoreCase("default")) {
		final Iterator<Entry<String, Grouping>> innerIt = componentGroup
			.entrySet().iterator();
		while (innerIt.hasNext()) {
		    final Map.Entry<String, Grouping> innerPair = innerIt
			    .next();
		    final String componentId = innerPair.getKey();
		    // Grouping group = innerPair.getValue();
		    // if (group.is_set_direct()){
		    result.addAll(tc.getComponentTasks(componentId));
		    // }
		}
	    }
	}
	return result;
    }

    // Previously HASH_DELIMITER = "-" in SystemParameters, but now is the same
    // as DIP_GLOBAL_ADD_DELIMITER
    // we need it for preaggregation
    public static String getColumnDelimiter(Map map) {
	return SystemParameters.getString(map, "DIP_GLOBAL_ADD_DELIMITER");
    }

    public static int getCompBatchSize(String compName, Map map) {
	return SystemParameters.getInt(map, compName + "_BS");
    }

    public static Type getDominantNumericType(List<ValueExpression> veList) {
	Type wrapper = veList.get(0).getType();
	for (int i = 1; i < veList.size(); i++) {
	    final Type currentType = veList.get(1).getType();
	    if (isDominant(currentType, wrapper))
		wrapper = currentType;
	}
	return wrapper;
    }

    public static String getHistogramFilename(Map conf, int numJoiners,
	    String filePrefix) {
	return getHistogramFilename(conf, filePrefix) + "_" + numJoiners + "j";
    }

    public static String getHistogramFilename(Map conf, String filePrefix) {
	String queryId = MyUtilities.getQueryID(conf);
	return SystemParameters.getString(conf, HistogramType.ROOT_DIR) + "/"
		+ filePrefix + "_" + queryId;
    }

    public static String getKeyRegionFilename(Map conf) {
	String queryId = MyUtilities.getQueryID(conf);
	return SystemParameters.getString(conf, "DIP_KEY_REGION_ROOT") + "/"
		+ queryId;
    }

    public static String getKeyRegionFilename(Map conf, String shortName) {
	return getKeyRegionFilename(conf) + "_" + shortName;
    }

    public static String getKeyRegionFilename(Map conf, String shortName,
	    int numJoiners) {
	return getKeyRegionFilename(conf, shortName) + "_" + numJoiners + "j";
    }

    public static String getKeyRegionFilename(Map conf, String shortName,
	    int numJoiners, int numBuckets) {
	return getKeyRegionFilename(conf, shortName, numJoiners) + "_"
		+ numBuckets + "b";
    }

    public static long getM(int firstRelSize, int secondRelSize, Map conf) {
	if (!SystemParameters.isExisting(conf, "M_DESCRIPTOR")) {
	    throw new RuntimeException(
		    "M_DESCRIPTOR does not exist in the config file!");
	}

	String mDescriptor = SystemParameters.getString(conf, "M_DESCRIPTOR");
	if (mDescriptor.equalsIgnoreCase("M")) {
	    return SystemParameters.getLong(conf, "TOTAL_OUTPUT_SIZE");
	} else if (mDescriptor.equalsIgnoreCase("MAX_INPUT")) {
	    return firstRelSize > secondRelSize ? firstRelSize : secondRelSize;
	} else if (mDescriptor.equalsIgnoreCase("SUM_INPUT")) {
	    return firstRelSize + secondRelSize;
	} else {
	    throw new RuntimeException("Unsupported getM " + mDescriptor);
	}
    }

    public static int getMax(int first, int second) {
	return first > second ? first : second;
    }

    public static long getMax(long first, long second) {
	return first > second ? first : second;
    }

    public static int getMin(int first, int second) {
	return first < second ? first : second;
    }

    public static long getMin(long first, long second) {
	return first < second ? first : second;
    }

    public static int getNumParentTasks(TopologyContext tc,
	    List<StormEmitter> emittersList) {
	int result = 0;
	for (final StormEmitter emitter : emittersList) {
	    // We have multiple emitterIDs only for StormSrcJoin
	    final String[] ids = emitter.getEmitterIDs();
	    for (final String id : ids)
		result += tc.getComponentTasks(id).size();
	}
	return result;
    }

    // used for NoACK optimization
    public static int getNumParentTasks(TopologyContext tc,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	return getNumParentTasks(tc, emittersList);
    }

    // used for NoACK optimization for StormSrcJoin
    public static int getNumParentTasks(TopologyContext tc,
	    StormSrcHarmonizer harmonizer) {
	final String id = String.valueOf(harmonizer.getID());
	return tc.getComponentTasks(String.valueOf(id)).size();
    }

    /*
     * Method invoked with arguments "a/b//c/e//f", 0 return "f" Method invoked
     * with arguments "a/b//c/e//f", 1 return "e"
     */
    public static String getPartFromEnd(String path, int fromEnd) {
	final String parts[] = path.split("\\/+");
	final int length = parts.length;
	return new String(parts[length - (fromEnd + 1)]);
    }

    public static String getQueryID(Map map) {
	String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
	String dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
	String sizeSkew = extractSizeSkew(dataPath);
	return queryName + "_" + sizeSkew;
    }

    public static String getStackTrace(Throwable aThrowable) {
	final Writer result = new StringWriter();
	final PrintWriter printWriter = new PrintWriter(result);
	aThrowable.printStackTrace(printWriter);
	return result.toString();
    }

    public static double getUsedMemoryMBs() {
	Runtime runtime = Runtime.getRuntime();
	long memory = runtime.totalMemory() - runtime.freeMemory();
	return memory / 1024.0 / 1024.0;
    }

    // assumes TreeMap where non-mentioned elements have value of their closest
    // left neighbor
    public static <K, V> V getValue(TreeMap<K, V> treeMap, K key) {
	Entry<K, V> repositionedEntry = treeMap.floorEntry(key);
	if (repositionedEntry == null) {
	    return null;
	} else {
	    return repositionedEntry.getValue();
	}
    }

    public static long getWindowClockTicker(Map conf) {
	if (SystemParameters.doesExist(conf, "WINDOW_GC_CLOCK_TICK_SECS"))
	    return SystemParameters.getLong(conf, "WINDOW_GC_CLOCK_TICK_SECS");
	else
	    return -1;
    }

    public static long getWindowSize(Map conf) {
	if (SystemParameters.doesExist(conf, "WINDOW_SIZE_SECS"))
	    return SystemParameters.getLong(conf, "WINDOW_SIZE_SECS") * 1000;
	else
	    return -1;
    }

    public static long getWindowTumblingSize(Map conf) {
	if (SystemParameters.doesExist(conf, "WINDOW_TUMBLING_SIZE_SECS"))
	    return SystemParameters.getLong(conf, "WINDOW_TUMBLING_SIZE_SECS") * 1000;
	else
	    return -1;
    }

    // if this is false, we have a specific mechanism to ensure all the tuples
    // are fully processed
    // it is based on CustomStreamGrouping
    public static boolean isAckEveryTuple(Map map) {
	int ackers;
	if (!SystemParameters.isExisting(map, "DIP_NUM_ACKERS"))
	    // number of ackers is defined in storm.yaml
	    ackers = SystemParameters.DEFAULT_NUM_ACKERS;
	else
	    ackers = SystemParameters.getInt(map, "DIP_NUM_ACKERS");
	return (ackers > 0);
    }

    public static boolean isAggBatchOutputMode(long batchOutputMillis) {
	return batchOutputMillis != 0L;
    }

    public static boolean isAutoOutputSampleSize(Map conf) {
	String mode = SystemParameters.getString(conf,
		"OUTPUT_SAMPLE_SIZE_MODE");
	return mode.equalsIgnoreCase("AUTO");
    }

    public static boolean isBDB(Map conf) {
	return SystemParameters.isExisting(conf, "DIP_IS_BDB")
		&& SystemParameters.getBoolean(conf, "DIP_IS_BDB");
    }

    public static boolean isBDBSkewed(Map conf) {
	return SystemParameters.isExisting(conf, "DIP_BDB_TYPE")
		&& SystemParameters.getString(conf, "DIP_BDB_TYPE")
			.equalsIgnoreCase("SKEWED");
    }

    public static boolean isBDBUniform(Map conf) {
	return SystemParameters.isExisting(conf, "DIP_BDB_TYPE")
		&& SystemParameters.getString(conf, "DIP_BDB_TYPE")
			.equalsIgnoreCase("UNIFORM");
    }

    public static <JAT extends Comparable<JAT>> boolean isCandidateRegion(
	    JoinMatrix<JAT> joinMatrix, Region region, ComparisonPredicate cp,
	    Map map) {
	int x1 = region.get_x1();
	int y1 = region.get_y1();
	int x2 = region.get_x2();
	int y2 = region.get_y2();

	boolean isSample = SystemParameters.getBoolean(map,
		"DIP_SAMPLE_STATISTICS");
	JAT kx1, ky1, kx2, ky2;
	if (isSample) {

	    // obtain keys from positions: both keys and positions are inclusive
	    NumericType wrapper = joinMatrix.getWrapper();
	    if (x1 == 0) {
		kx1 = (JAT) wrapper.getMinValue();
	    } else {
		kx1 = joinMatrix.getJoinAttributeX(x1);
	    }
	    if (y1 == 0) {
		ky1 = (JAT) wrapper.getMinValue();
	    } else {
		ky1 = joinMatrix.getJoinAttributeY(y1);
	    }
	    if (x2 == joinMatrix.getXSize() - 1) {
		kx2 = (JAT) wrapper.getMaxValue();
	    } else {
		kx2 = joinMatrix.getJoinAttributeX(x2);
		JAT kx2Next = joinMatrix.getJoinAttributeX(x2 + 1);
		if (!kx2.equals(kx2Next)) {
		    // unless they share the boundary key,
		    // this bucket is responsible for all the values just before
		    // another bucket starts
		    kx2 = (JAT) wrapper.minDecrement(kx2Next);
		}
	    }
	    if (y2 == joinMatrix.getYSize() - 1) {
		ky2 = (JAT) wrapper.getMaxValue();
	    } else {
		ky2 = joinMatrix.getJoinAttributeY(y2);
		JAT ky2Next = joinMatrix.getJoinAttributeY(y2 + 1);
		if (!ky2.equals(ky2Next)) {
		    // unless they share the boundary key,
		    // this bucket is responsible for all the values just before
		    // another bucket starts
		    ky2 = (JAT) wrapper.minDecrement(ky2Next);
		}
	    }
	} else {
	    kx1 = joinMatrix.getJoinAttributeX(x1);
	    ky1 = joinMatrix.getJoinAttributeY(y1);
	    kx2 = joinMatrix.getJoinAttributeX(x2);
	    ky2 = joinMatrix.getJoinAttributeY(y2);
	}

	// kx2 and ky2 are inclusive; that's what cp.isCandidateRegion expects
	return cp.isCandidateRegion(kx1, ky1, kx2, ky2);
    }

    public static boolean isCustomTimestampMode(Map map) {
	return SystemParameters.isExisting(map, "CUSTOM_TIMESTAMP")
		&& SystemParameters.getBoolean(map, "CUSTOM_TIMESTAMP");
    }

    /*
     * Does bigger dominates over smaller? For (bigger, smaller) = (double,
     * long) answer is yes.
     */
    private static boolean isDominant(Type bigger, Type smaller) {
	// for now we only have two numeric types: double and long
	if (bigger instanceof DoubleType)
	    return true;
	else
	    return false;
    }

    public static boolean isFinalAck(List<String> tuple, Map map) {
	return (!isAckEveryTuple(map)) && isFinalAck(tuple.get(0));
    }

    private static boolean isFinalAck(String tupleString) {
	return tupleString.equals(SystemParameters.LAST_ACK);
    }

    public static boolean isFinalAckManualBatching(String tupleString, Map map) {
	return (!isAckEveryTuple(map)) && isFinalAck(tupleString);
    }

    public static boolean isIIPrecBoth(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	return (SystemParameters.isExisting(conf, key))
		&& SystemParameters.getString(conf, key).equalsIgnoreCase(
			"BOTH");
    }

    public static boolean isIIPrecDense(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	if (SystemParameters.isExisting(conf, key)) {
	    return (SystemParameters.getString(conf, key).equalsIgnoreCase(
		    "DENSE") || SystemParameters.getString(conf, key)
		    .equalsIgnoreCase("BOTH"));
	} else {
	    // by default it is turned false, because it's expensive
	    return false;
	}
    }

    public static boolean isIIPrecPWeight(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	// if nothing is in there, we take the best case
	return (!(SystemParameters.isExisting(conf, key))
		|| SystemParameters.getString(conf, key).equalsIgnoreCase(
			"PWEIGHT") || SystemParameters.getString(conf, key)
		.equalsIgnoreCase("BOTH"));
    }

    public static boolean isManualBatchingMode(Map map) {
	return SystemParameters.isExisting(map, "BATCH_SEND_MODE")
		&& SystemParameters.getString(map, "BATCH_SEND_MODE")
			.equalsIgnoreCase("MANUAL_BATCH");
    }

    public static <K, V> boolean isMapsEqual(HashMap<K, V> map1,
	    Map<K, V> map2, StringBuilder sb) {
	int size1 = map1.size();
	int size2 = map2.size();
	if (size1 != size2) {
	    sb.append("Different sizes: Size1 = " + size1 + ", size2 = "
		    + size2);
	    return false;
	}
	for (Map.Entry<K, V> entry : map1.entrySet()) {
	    K key1 = entry.getKey();
	    V value1 = entry.getValue();
	    if (!map2.containsKey(key1)) {
		sb.append("Map2 does not contain key " + key1);
		return false;
	    } else {
		V value2 = map2.get(key1);
		if (!value1.equals(value2)) {
		    sb.append("Different values for key = " + key1
			    + ": value1 = " + value1 + ", value2 = " + value2);
		    return false;
		}
	    }
	}
	return true;
    }

    // If Opt2 is not used (S1ReservoirGenerator), let's not invoke this method
    // map1 is based on prefix sum: the value of a key is the value of its
    // closest neighbor to the left
    public static <K, V> boolean isMapsEqual(TreeMap<K, V> map1,
	    Map<K, V> map2, StringBuilder sb) {
	// their sizes differ because of different map implementation
	for (Map.Entry<K, V> entry : map2.entrySet()) {
	    K key2 = entry.getKey();
	    V value2 = entry.getValue();
	    V value1 = getValue(map1, key2);
	    if (!value2.equals(value1)) {
		sb.append("Different values for key = " + key2 + ": value1 = "
			+ value1 + ", value2 = " + value2);
		return false;
	    }
	}
	return true;
    }

    public static boolean isOutputSampleSize(List<String> tuple) {
	return tuple.get(0).equals(SystemParameters.OUTPUT_SAMPLE_SIZE);
    }

    // printout the next_to_last component output just after the selection
    public static boolean isPrintFilteredLast(Map map) {
	return SystemParameters.isExisting(map, "PRINT_FILTERED_LAST")
		&& SystemParameters.getBoolean(map, "PRINT_FILTERED_LAST");
    }

    public static boolean isPrintLatency(int hierarchyPosition, Map conf) {
	return MyUtilities.isCustomTimestampMode(conf)
		&& hierarchyPosition == StormComponent.FINAL_COMPONENT;
    }

    public static boolean isRelSize(List<String> tuple) {
	return tuple.get(0).equals(SystemParameters.REL_SIZE);
    }

    public static boolean isSending(int hierarchyPosition,
	    long batchOutputMillis) {
	return (hierarchyPosition != StormComponent.FINAL_COMPONENT)
		&& !isAggBatchOutputMode(batchOutputMillis);
    }

    public static boolean isStatisticsCollector(Map map, int hierarchyPosition) {
	return hierarchyPosition == StormComponent.FINAL_COMPONENT
		&& SystemParameters.isExisting(map, "DIP_STATISTIC_COLLECTOR")
		&& SystemParameters.getBoolean(map, "DIP_STATISTIC_COLLECTOR");
    }

    public static boolean isStoreTimestamp(Map map, int hierarchyPosition) {
	return (isWindowTimestampMode(map))
		|| (isCustomTimestampMode(map)
			&& hierarchyPosition == StormComponent.FINAL_COMPONENT
			&& SystemParameters.isExisting(map, "STORE_TIMESTAMP") && SystemParameters
			    .getBoolean(map, "STORE_TIMESTAMP"));
    }

    public static boolean isThrottlingMode(Map map) {
	return SystemParameters.isExisting(map, "BATCH_SEND_MODE")
		&& SystemParameters.getString(map, "BATCH_SEND_MODE")
			.equalsIgnoreCase("THROTTLING");
    }

    public static boolean isTickTuple(Tuple tuple) {
	return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
		&& tuple.getSourceStreamId().equals(
			Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static boolean isTotalOutputSize(List<String> tuple) {
	return tuple.get(0).equals(SystemParameters.TOTAL_OUTPUT_SIZE);
    }

    public static boolean isWindowTimestampMode(Map map) {
	return (WindowSemanticsManager._IS_WINDOW_SEMANTICS);
    }

    public static List<String> listFilesForPath(String dir) {
	List<String> filePaths = new ArrayList<String>();

	File folder = new File(dir);
	for (File fileEntry : folder.listFiles()) {
	    if (fileEntry.isDirectory()) {
		if (!fileEntry.getName().startsWith(".")) {
		    // avoid hidden folder
		    filePaths.addAll(listFilesForPath(fileEntry
			    .getAbsolutePath()));
		}
	    } else {
		filePaths.add(fileEntry.getAbsolutePath());
	    }
	}

	return filePaths;
    }

    public static <T extends Comparable<T>> List<ValueExpression> listTypeErasure(
	    List<ValueExpression<T>> input) {
	final List<ValueExpression> result = new ArrayList<ValueExpression>();
	for (final ValueExpression ve : input)
	    result.add(ve);
	return result;
    }

    public static void main(String[] args) {
	System.out
		.println("For relSize = 12 000 and ns = 5821, newNumOfBuckets = "
			+ MyUtilities.adjustPartitioning(12000, 5821, "n_s"));
	System.out
		.println("For relSize = 12 000 and ns = 6000, newNumOfBuckets = "
			+ MyUtilities.adjustPartitioning(12000, 6000, "n_s"));
	System.out
		.println("For relSize = 12 000 and ns = 6001, newNumOfBuckets = "
			+ MyUtilities.adjustPartitioning(12000, 6001, "n_s"));
	System.out
		.println("For relSize = 12M and ns = 12405, newNumOfBuckets = "
			+ MyUtilities
				.adjustPartitioning(12000000, 12405, "n_s"));
	System.out
		.println("For relSize = 25123111 and ns = 17778, newNumOfBuckets = "
			+ MyUtilities
				.adjustPartitioning(25123111, 17778, "n_s"));
    }

    public static void printBlockingResult(String componentName,
	    Operator op, int hierarchyPosition, Map map, Logger log) {
	// just print it, necessary for both modes (in Local mode we might print
	// other than final components)
	printPartialResult(componentName, op.getNumTuplesProcessed(),
		op.printContent(), map, log);

	LocalMergeResults.localCollectFinalResult(op, hierarchyPosition, map,
		log);
    }

    private static void printPartialResult(String componentName,
	    int numProcessedTuples, String compContent, Map map, Logger log) {
	final StringBuilder sb = new StringBuilder();
	sb.append("\nThe result for topology ");
	sb.append(SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
	sb.append("\nComponent ").append(componentName).append(":\n");
	sb.append("\nThis task received ").append(numProcessedTuples);
	sb.append("\n").append(compContent);
	log.info(sb.toString());
    }

    // in ProcessFinalAck and dumpSignal we have acking at the end, because we
    // return after that
    public static void processFinalAck(int numRemainingParents,
	    int hierarchyPosition, Map conf, Tuple stormTupleRcv,
	    OutputCollector collector) {
	if (numRemainingParents == 0)
	    // this task received from all the parent tasks
	    // SystemParameters.LAST_ACK
	    if (hierarchyPosition != StormComponent.FINAL_COMPONENT) {
		// if this component is not the last one
		final Values values = createUniversalFinalAckTuple(conf);
		collector.emit(values);
	    } else
		collector.emit(SystemParameters.EOF_STREAM, new Values(
			SystemParameters.EOF));
	collector.ack(stormTupleRcv);
    }

    public static void processFinalAck(int numRemainingParents,
	    int hierarchyPosition, Map conf, Tuple stormTupleRcv,
	    OutputCollector collector, PeriodicAggBatchSend periodicBatch) {
	if (numRemainingParents == 0)
	    if (periodicBatch != null) {
		periodicBatch.cancel();
		periodicBatch.getComponent().aggBatchSend();
	    }
	processFinalAck(numRemainingParents, hierarchyPosition, conf,
		stormTupleRcv, collector);
    }

    public static void processFinalAckCustomStream(String streamId,
	    int numRemainingParents, int hierarchyPosition, Map conf,
	    Tuple stormTupleRcv, OutputCollector collector) {
	if (numRemainingParents == 0)
	    // this task received from all the parent tasks
	    // SystemParameters.LAST_ACK
	    if (hierarchyPosition != StormComponent.FINAL_COMPONENT) {
		// if this component is not the last one
		final Values values = createUniversalFinalAckTuple(conf);
		// collector.emit(values); this caused a bug that
		// S1ReservoirGenerator sent to S1ReservoirMerge 2x more acks
		// than required
		collector.emit(streamId, values);
	    } else
		collector.emit(SystemParameters.EOF_STREAM, new Values(
			SystemParameters.EOF));
	collector.ack(stormTupleRcv);
    }

    public static void processFinalAckCustomStream(String streamId,
	    int numRemainingParents, int hierarchyPosition, Map conf,
	    Tuple stormTupleRcv, OutputCollector collector,
	    PeriodicAggBatchSend periodicBatch) {
	if (numRemainingParents == 0)
	    if (periodicBatch != null) {
		periodicBatch.cancel();
		periodicBatch.getComponent().aggBatchSend();
	    }
	processFinalAckCustomStream(streamId, numRemainingParents,
		hierarchyPosition, conf, stormTupleRcv, collector);
    }

    /*
     * Read query plans - read as verbatim
     */
    public static String readFile(String path) {
	try {
	    final StringBuilder sb = new StringBuilder();
	    final BufferedReader reader = new BufferedReader(new FileReader(
		    new File(path)));

	    String line;
	    while ((line = reader.readLine()) != null)
		sb.append(line).append("\n");
	    if (sb.length() > 0)
		sb.deleteCharAt(sb.length() - 1); // last \n is unnecessary
	    reader.close();

	    return sb.toString();
	} catch (final IOException ex) {
	    final String err = MyUtilities.getStackTrace(ex);
	    throw new RuntimeException("Error while reading a file:\n " + err);
	}
    }

    /*
     * Used for reading a result file, # should be treated as possible data, not
     * comment
     */
    public static List<String> readFileLinesSkipEmpty(String path)
	    throws IOException {
	final BufferedReader reader = new BufferedReader(new FileReader(
		new File(path)));

	final List<String> lines = new ArrayList<String>();
	String strLine;
	while ((strLine = reader.readLine()) != null)
	    if (!strLine.isEmpty())
		lines.add(strLine);
	reader.close();
	return lines;
    }

    /*
     * Used for reading an SQL file
     */
    public static String readFileSkipEmptyAndComments(String path) {
	try {
	    final StringBuilder sb = new StringBuilder();

	    final List<String> lines = readFileLinesSkipEmpty(path);
	    for (String line : lines) {
		line = line.trim();
		if (!line.startsWith("#"))
		    sb.append(line).append(" ");
	    }
	    if (sb.length() > 0)
		sb.deleteCharAt(sb.length() - 1); // last space is unnecessary

	    return sb.toString();
	} catch (final IOException ex) {
	    final String err = MyUtilities.getStackTrace(ex);
	    throw new RuntimeException("Error while reading a file:\n " + err);
	}
    }

    public static void sendTuple(String streamId, Values stormTupleSnd,
	    Tuple stormTupleRcv, OutputCollector collector, Map conf) {
	// stormTupleRcv is equals to null when we send tuples in batch fashion
	if (isAckEveryTuple(conf) && stormTupleRcv != null)
	    collector.emit(streamId, stormTupleRcv, stormTupleSnd);
	else
	    collector.emit(streamId, stormTupleSnd);
    }

    // this is for Spout
    public static void sendTuple(Values stormTupleSnd,
	    SpoutOutputCollector collector, Map conf) {
	String msgId = null;
	if (MyUtilities.isAckEveryTuple(conf))
	    msgId = "T"; // as short as possible
	if (msgId != null)
	    collector.emit(stormTupleSnd, msgId);
	else
	    collector.emit(stormTupleSnd);
    }
    
 // this is for Spout
    public static void sendTuple(String streamID, Values stormTupleSnd,
	    SpoutOutputCollector collector, Map conf) {
	String msgId = null;
	if (MyUtilities.isAckEveryTuple(conf))
	    msgId = "T"; // as short as possible
	if (msgId != null)
	    collector.emit(streamID, stormTupleSnd, msgId);
	else
	    collector.emit(streamID, stormTupleSnd);
    }

    /*
     * no acking at the end, because for one tuple arrived in JoinComponent, we
     * might have multiple tuples to be sent.
     */
    public static void sendTuple(Values stormTupleSnd, Tuple stormTupleRcv,
	    OutputCollector collector, Map conf) {
	// stormTupleRcv is equals to null when we send tuples in batch fashion
	if (isAckEveryTuple(conf) && stormTupleRcv != null)
	    collector.emit(stormTupleRcv, stormTupleSnd);
	else
	    collector.emit(stormTupleSnd);
    }

    private static void setParentPartitioner(Component component) {
	if (component instanceof DataSourceComponent) {
	    DataSourceComponent dsComp = (DataSourceComponent) component;
	    dsComp.setPartitioner(true);
	} else if (component instanceof ThetaJoinComponent) {
	    ThetaJoinComponent tComp = (ThetaJoinComponent) component;
	    tComp.setPartitioner(true);
	} else {
	    throw new RuntimeException("Unsupported component type "
		    + component);
	}
    }

    public static List<String> stringToTuple(String tupleString, Map conf) { // arraylist
	// 2
	// values
	final String[] columnValues = tupleString.split(SystemParameters
		.getString(conf, "DIP_GLOBAL_SPLIT_DELIMITER"));
        return new ArrayList<String>(Arrays.asList(columnValues));
    }

    public static InputDeclarer thetaAttachEmitterComponents(
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter emitter2, List<String> allCompNames,
	    MatrixAssignment assignment, Map map, Type wrapper) {

	// MatrixAssignment assignment = new MatrixAssignment(firstRelationSize,
	// secondRelationSize, parallelism,-1);

	final String firstEmitterIndex = String.valueOf(allCompNames
		.indexOf(emitter1.getName()));
	final String secondEmitterIndex = String.valueOf(allCompNames
		.indexOf(emitter2.getName()));

	CustomStreamGrouping mapping = null;

	if (assignment instanceof ContentSensitiveMatrixAssignment) {
	    mapping = new ContentSensitiveThetaJoinGrouping(firstEmitterIndex,
		    secondEmitterIndex, assignment, map, wrapper);
	} else
	    mapping = new ContentInsensitiveThetaJoinGrouping(
		    firstEmitterIndex, secondEmitterIndex, assignment, map);

	final ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.add(emitter2);

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID, mapping);
	}
	return currentBolt;
    }

    // FIXME
    // For DateIntegerConversion, we want 1992-02-01 instead of 19920201
    // and there the default toString method returns 19920201
    public static <JAT extends Comparable<JAT>> String toSpecialString(JAT key,
	    NumericType wrapper) {
	String strKey;
	if (wrapper instanceof DateIntegerType) {
	    DateIntegerType dateIntConv = (DateIntegerType) wrapper;
	    strKey = dateIntConv.toStringWithDashes((Integer) key);
	} else {
	    strKey = wrapper.toString(key);
	}
	return strKey;
    }

    public static String tupleToString(List<String> tuple, Map conf) {
	String tupleString = "";
	for (int i = 0; i < tuple.size(); i++)
	    if (i == tuple.size() - 1)
		tupleString += tuple.get(i);
	    else
		tupleString += tuple.get(i)
			+ SystemParameters.getString(conf,
				"DIP_GLOBAL_ADD_DELIMITER");
	// this cause a bug when space (" ") is the last character:
	// tupleString=tupleString.trim();
	return tupleString;
    }

    private static Logger LOG = Logger.getLogger(MyUtilities.class);

    public static final String SINGLE_HASH_KEY = "SingleHashEntry";

}

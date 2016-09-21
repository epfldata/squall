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

package ch.epfl.data.squall.storm_components.hyper_cube;

import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.storage.indexes.Index;
import ch.epfl.data.squall.storm_components.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Semaphore;

import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignerFactory;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignment;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.visitors.PredicateCreateIndexesVisitor;
import ch.epfl.data.squall.visitors.PredicateUpdateIndexesVisitor;
import gnu.trove.list.array.TIntArrayList;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.TupleStorage;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.statistics.StatisticsUtilities;


public class StormHyperCubeJoin extends StormBoltComponent {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormHyperCubeJoin.class);
    private List<TupleStorage> relationStorages;
    private List<String> emitterIndexes;
    private long numSentTuples = 0;
    private Map<String, Predicate> joinPredicates;

    private ChainOperator operatorChain;

    // For each relation that has predicate between them we create Index.
    // firstRelationIndexes and secondRelationIndexes store Indexes between two relations.
    // [Key, ArrayList[Index]] -> key = [FirstRelation+SecondRelation], string concatenation of two relation names
    // Same for secondRelationIndexes, key = [SecondRelation+FirstRelation].
    // Look at createIndexes() function for more understanding
    private Map<String, ArrayList<Index>> firstRelationIndexes = new HashMap<String, ArrayList<Index>>();
    private Map<String, ArrayList<Index>> secondRelationIndexes = new HashMap<String, ArrayList<Index>>();

    private Map<String, ArrayList<Integer>> operatorForIndexes = new HashMap<String, ArrayList<Integer>>();
    private Map<String, ArrayList<Object>> typeOfValueIndexed = new HashMap<String, ArrayList<Object>>();
    Map<String, List<String>> valuesToIndexMap = new HashMap<String, List<String>>();
    private boolean existIndexes = false;
    // for agg batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private long _aggBatchOutputMillis;

    // for printing statistics for creating graphs
    protected Calendar _cal = Calendar.getInstance();
    protected DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    protected SimpleDateFormat _format = new SimpleDateFormat(
            "EEE MMM d HH:mm:ss zzz yyyy");
    protected StatisticsUtilities _statsUtils;

    public StormHyperCubeJoin (ArrayList<StormEmitter> emitters, ComponentProperties cp,
                               List<String> allCompNames, Map<String, Predicate> joinPredicates, int hierarchyPosition,
                               TopologyBuilder builder, TopologyKiller killer, Config conf, Type wrapper) {

        super(cp, allCompNames, hierarchyPosition, false, conf);

        emitterIndexes = new ArrayList<String>();
        for (int i = 0; i < emitters.size(); i++) {
            emitterIndexes.add(String.valueOf(allCompNames.indexOf(emitters.get(i).getName())));
        }

        _aggBatchOutputMillis = cp.getBatchOutputMillis();
        _statsUtils = new StatisticsUtilities(getConf(), LOG);
        final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
        operatorChain = cp.getChainOperator();
        this.joinPredicates = joinPredicates;
        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

        final HyperCubeAssignment _currentMappingAssignment;
        long[] cardinality = new long[emitters.size()];
        for (int i = 0; i < emitters.size(); i++)
            cardinality[i] = SystemParameters.getInt(conf, emitters.get(i).getName() + "_CARD");
        _currentMappingAssignment = new HyperCubeAssignerFactory().getAssigner(parallelism, cardinality);

        currentBolt = MyUtilities.attachEmitterHyperCube(currentBolt,
                    emitters, allCompNames,
                    _currentMappingAssignment, conf);

        if (getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf)))
            killer.registerComponent(this, parallelism);

        if (cp.getPrintOut() && operatorChain.isBlocking())
            currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);

        relationStorages = new ArrayList<TupleStorage>();
        for (int i = 0; i < emitters.size(); i++)
            relationStorages.add(new TupleStorage());


        if (joinPredicates != null) {
            createIndexes();
            existIndexes = true;
        } else
            existIndexes = false;

    }
    @Override
    public void aggBatchSend() {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            if (operatorChain != null) {
                final Operator lastOperator = operatorChain.getLastOperator();
                if (lastOperator instanceof AggregateOperator) {
                    try {
                        _semAgg.acquire();
                    } catch (final InterruptedException ex) {
                    }
                    // sending
                    final AggregateOperator agg = (AggregateOperator) lastOperator;
                    final List<String> tuples = agg.getContent();
                    for (final String tuple : tuples)
                        tupleSend(MyUtilities.stringToTuple(tuple, getConf()),
                                null, 0);
                    // clearing
                    agg.clearStorage();
                    _semAgg.release();
                }
            }
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv,
                                         List<String> inTuple, long lineageTimestamp, boolean isLastInBatch) {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            try {
                _semAgg.acquire();
            } catch (final InterruptedException ex) {
            }
        for (List<String> tuple : operatorChain.process(inTuple, lineageTimestamp)) {
          if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            _semAgg.release();
          if (tuple == null)
            return;
          numSentTuples++;
          printTuple(tuple);
          if (numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
            printStatistics(SystemParameters.OUTPUT_PRINT);
          if (MyUtilities
              .isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
            long timestamp = 0;
            if (MyUtilities.isCustomTimestampMode(getConf()))
        	// if measuring latency of the last operator only
                // if (getHierarchyPosition() == StormComponent.NEXT_TO_LAST_COMPONENT)
                //       timestamp = System.currentTimeMillis();
        	timestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP); // the timestamp of the tuple that comes last
        	
            tupleSend(tuple, stormTupleRcv, timestamp);
          }
          if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf()))
            printTupleLatency(numSentTuples - 1, lineageTimestamp); // TODO - this is always 0 in the code
        }
    }


    // For each emitter we check is there Predicate between them.
    // If yes, we create Indexes between them and add to firstRelationIndexes and secondRelationIndexes.
    private void createIndexes() {
        for (int i = 0; i < emitterIndexes.size(); i++) {
          for (int j = i + 1; j < emitterIndexes.size(); j++) {
              String key = emitterIndexes.get(i) + emitterIndexes.get(j);
              String keyReverse = emitterIndexes.get(j) + emitterIndexes.get(i);
              if (joinPredicates.containsKey(key)) {
                  Predicate pr = joinPredicates.get(key);
                  final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
                  pr.accept(visitor);
                  firstRelationIndexes.put(key, new ArrayList<Index>(visitor._firstRelationIndexes));
                  secondRelationIndexes.put(keyReverse, new ArrayList<Index>(visitor._secondRelationIndexes));

                  operatorForIndexes.put(key, new ArrayList<Integer>(visitor._operatorForIndexes));
                  typeOfValueIndexed.put(key, new ArrayList<Object>(visitor._typeOfValueIndexed));
              }
          }
        }
    }

    @Override
    public void execute(Tuple stormTupleRcv) {
        if (_firstTime
                && MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)) {
            _periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis,
                    this);
            _firstTime = false;
        }

        if (receivedDumpSignal(stormTupleRcv)) {
            MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
            return;
        }

        if (!MyUtilities.isManualBatchingMode(getConf())) {
            final String inputComponentIndex = stormTupleRcv
                    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
            final List<String> tuple = (List<String>) stormTupleRcv.getValueByField(StormComponent.TUPLE); // getValue(1);
            final String inputTupleHash = stormTupleRcv.getStringByField(StormComponent.HASH);// getString(2);
            if (processFinalAck(tuple, stormTupleRcv))
                return;
            final String inputTupleString = MyUtilities.tupleToString(tuple, getConf());
            processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, true);
        } else {
            final String inputComponentIndex = stormTupleRcv
                    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
            final String inputBatch = stormTupleRcv
                    .getStringByField(StormComponent.TUPLE);// getString(1);
            final String[] wholeTuples = inputBatch
                    .split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
            final int batchSize = wholeTuples.length;
            for (int i = 0; i < batchSize; i++) {
                // parsing
                final String currentTuple = new String(wholeTuples[i]);
                final String[] parts = currentTuple.split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
                String inputTupleHash = null;
                String inputTupleString = null;
                if (parts.length == 1)
                    // lastAck
                    inputTupleString = new String(parts[0]);
                else {
                    inputTupleHash = new String(parts[0]);
                    inputTupleString = new String(parts[1]);
                }
                final List<String> tuple = MyUtilities.stringToTuple(
                        inputTupleString, getConf());
                // final Ack check
                if (processFinalAck(tuple, stormTupleRcv)) {
                    if (i != batchSize - 1)
                        throw new RuntimeException(
                                "Should not be here. LAST_ACK is not the last tuple!");
                    return;
                }
                // processing a tuple
                if (i == batchSize - 1)
                    processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, true);
                else
                    processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, false);
            }
        }
        getCollector().ack(stormTupleRcv);
    }

    @Override
    public ChainOperator getChainOperator() {
        return operatorChain;
    }

    // from IRichBolt
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return getConf();
    }

    @Override
    public String getInfoID() {
        final String str = "DestinationStorage " + getID() + " has ID: "
                + getID();
        return str;
    }

    @Override
    public long getNumSentTuples() {
        return numSentTuples;
    }

    @Override
    public PeriodicAggBatchSend getPeriodicAggBatch() {
        return _periodicAggBatch;
    }

    protected void performJoin(Tuple stormTupleRcv, List<String> tuple, int rowID,
                               String emitterIndex, boolean isLastInBatch) {

        ArrayList<Integer> result = new ArrayList<Integer>();
        List<List<String>> outputTuples = new ArrayList<List<String>>();
        result.add(rowID);
        // We have tuple from C. getOrderToJoin should return optimal join order for joining, e.g C - A - B - D - E
        ArrayList<Integer> joinOrder = getOrderToJoin(emitterIndex);
        join(1, joinOrder, result, outputTuples);

        long lineageTimestamp = 0;
        for (List<String> tpl : outputTuples) {
            applyOperatorsAndSend(stormTupleRcv, tpl,
                    lineageTimestamp, isLastInBatch);
        }
      }

    // it is dummy function - Patrice is going to change arguments and return paramater.
    // it analyze join graph and returns arraylist for join order
    public ArrayList<Integer> getOrderToJoin(String emitterIndex) {
        return new ArrayList<Integer>();
    }

    // RelationVisited - number of relations already joined from the left
    // JoinOrder - the order we should use to join relations, e.g A - B - C - D - E or A - C - D - B - E
    // JoinResult - if we have  RelationVisited = 3, it means we have already joined there relations from the left
    //              according JoinOrder.
    // OutputTuples - is aggregator, to collect the final result
    public void join(int relationVisited, ArrayList<Integer> joinOrder, ArrayList<Integer> joinResult, List<List<String>> outputTuples) {
        // we have rowA, rowB, rowC, rowD, rowE => we can create tuple
        if (relationVisited == relationStorages.size()) {
            List<List<String>> tuple = new ArrayList<List<String>>();
            for (int i = 0; i < joinResult.size(); i++) {
                String oppositeTupleString = relationStorages.get(joinOrder.get(i)).get(joinResult.get(joinOrder.get(i)));
                final List<String> oppositeTuple = MyUtilities.stringToTuple(
                        oppositeTupleString, getComponentConfiguration());
                tuple.add(oppositeTuple);
                List<String> outputTuple = MyUtilities.createOutputTuple(tuple);
                outputTuples.add(outputTuple);
            }
            // no need to continue
            return;
        }

        // We have some tuple till me, for example in join result we have  C - B - D
        // Now I should choose all possible tuples form A.
        // We look A predicates and choose A - C, A - B, and A - D and intersect them
        LinkedList<Integer> tuplesToJoin = new LinkedList<Integer>();
        boolean firsTime = true;
        for (int i = 0; i < relationVisited; i++) {
            LinkedList<Integer> tmpTuplesToJoin = new LinkedList<Integer>();
            selectTupleToJoinForMultipleJoin(joinOrder.get(i), joinOrder.get(relationVisited), tmpTuplesToJoin);

            if (firsTime)
                tuplesToJoin = tmpTuplesToJoin;
            else
                tuplesToJoin.retainAll(tmpTuplesToJoin);
        }

        // if empty we should not continue
        if (tuplesToJoin.isEmpty()) return;

        for (int id : tuplesToJoin) {
            ArrayList<Integer> newJoinResult = (ArrayList<Integer>)joinResult.clone();
            newJoinResult.add(id);
            join(relationVisited + 1, joinOrder, newJoinResult, outputTuples);
        }
    }

    private void selectTupleToJoinForMultipleJoin(int firstEmitterIndex, int secondEmitterIndex, List<Integer> tuplesToJoin) {
        boolean isFromFirstEmitter;
        List<Index> oppositeIndexes;
        String key = emitterIndexes.get(firstEmitterIndex) + emitterIndexes.get(secondEmitterIndex);
        String keyReverse = emitterIndexes.get(secondEmitterIndex) + emitterIndexes.get(firstEmitterIndex);
        if (firstRelationIndexes.containsKey(key)) {
            oppositeIndexes = firstRelationIndexes.get(key);
            isFromFirstEmitter = true;
        } else {
            oppositeIndexes = secondRelationIndexes.get(keyReverse);
            isFromFirstEmitter = false;
            key = keyReverse;
        }
        Predicate pr = joinPredicates.get(key);
        final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
        pr.accept(visitor);
        selectTupleToJoin(key, oppositeIndexes, isFromFirstEmitter, tuplesToJoin);
    }

    private void selectTupleToJoin(String key, List<Index> oppositeIndexes, boolean isFromFirstEmitter,
                                   List<Integer> tuplesToJoin) {

        final TIntArrayList rowIds = new TIntArrayList();
        // If there is at least one index (so we have single join conditions with
        // 1 index per condition)
        // Get the row indices in the storage of the opposite relation that
        // satisfy each join condition (equijoin / inequality)
        // Then take the intersection of the returned row indices since each
        // join condition
        // is separated by AND

        for (int i = 0; i < oppositeIndexes.size(); i++) {
            TIntArrayList currentRowIds = null;

            final Index currentOpposIndex = oppositeIndexes.get(i);
            final String value = valuesToIndexMap.get(key).get(i);

            int currentOperator = operatorForIndexes.get(key).get(i);
            // Switch inequality operator if the tuple coming is from the other
            // relation
            if (isFromFirstEmitter) {
                final int operator = currentOperator;

                if (operator == ComparisonPredicate.GREATER_OP)
                    currentOperator = ComparisonPredicate.LESS_OP;
                else if (operator == ComparisonPredicate.NONGREATER_OP)
                    currentOperator = ComparisonPredicate.NONLESS_OP;
                else if (operator == ComparisonPredicate.LESS_OP)
                    currentOperator = ComparisonPredicate.GREATER_OP;
                else if (operator == ComparisonPredicate.NONLESS_OP)
                    currentOperator = ComparisonPredicate.NONGREATER_OP;
                else
                    currentOperator = operator;
            }

            // Get the values from the index (check type first)
            if (typeOfValueIndexed.get(key).get(i) instanceof String)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        value);
                // Even if valueIndexed is at first time an integer with
                // precomputation a*col +b, it become a double
            else if (typeOfValueIndexed.get(key).get(i) instanceof Double)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        Double.parseDouble(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Integer)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        Integer.parseInt(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Long)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        Long.parseLong(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Date)
                try {
                    currentRowIds = currentOpposIndex.getValues(
                            currentOperator, _format.parse(value));
                } catch (final java.text.ParseException e) {
                    e.printStackTrace();
                }
            else
                throw new RuntimeException("non supported type");
            // Compute the intersection
            // TODO: Search only within the ids that are in rowIds from previous
            // conditions If
            // nothing returned (and since we want intersection), no need to
            // proceed.
            if (currentRowIds == null)
                return;
            // If it's the first index, add everything. Else keep the
            // intersection
            if (i == 0)
                rowIds.addAll(currentRowIds);
            else
                rowIds.retainAll(currentRowIds);
            // If empty after intersection, return
            if (rowIds.isEmpty())
                return;
        }
        // generate tuplestorage
        for (int i = 0; i < rowIds.size(); i++) {
            final int id = rowIds.get(i);
            tuplesToJoin.add(id);
        }
    }

    @Override
    protected void printStatistics(int type) {
    }

    private void processNonLastTuple(String inputComponentIndex,
                                     String inputTupleString, //
                                     List<String> tuple, // these two are the same
                                     Tuple stormTupleRcv, boolean isLastInBatch) {
        // Find out affected storage
        TupleStorage affectedStorage = null;
        for (int i = 0; i < emitterIndexes.size(); i++) {
            if (inputComponentIndex.equals(emitterIndexes.get(i))) {
                affectedStorage = relationStorages.get(i);
                break;
            }
        }
        // add the stormTuple to the specific storage
        if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
            final long incomingTimestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
            inputTupleString = incomingTimestamp
                    + SystemParameters.STORE_TIMESTAMP_DELIMITER
                    + inputTupleString;
        }
        final int row_id = affectedStorage.insert(inputTupleString);
        if (existIndexes)
            updateIndexes(inputComponentIndex, tuple, row_id);

        performJoin(stormTupleRcv, tuple, row_id, inputComponentIndex, isLastInBatch);
    }

    // The same architecture that is in StormJoinBoltComponent.
    // However, we consider that there can be many different predicates for given relation
    // with different other relations. So, we should update all of them.
    private void updateIndexes(String inputComponentIndex, List<String> tuple, int row_id) {

        for (int i = 0; i < emitterIndexes.size(); i++) {
            String key = inputComponentIndex + emitterIndexes.get(i);
            String keyReverse = emitterIndexes.get(i) + inputComponentIndex;

            if (firstRelationIndexes.containsKey(key)) {
                List<Index> affectedIndexes = firstRelationIndexes.get(key);
                final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(true, tuple);
                Predicate _joinPredicate = joinPredicates.get(key);
                _joinPredicate.accept(visitor);

                final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
                        visitor._typesOfValuesToIndex);
                final List<String> valuesToIndex = new ArrayList<String>(
                        visitor._valuesToIndex);

                for (int j = 0; j < affectedIndexes.size(); j++)
                    if (typesOfValuesToIndex.get(j) instanceof Integer)
                        affectedIndexes.get(j).put(row_id, Integer.parseInt(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Double)
                        affectedIndexes.get(j).put(row_id, Double.parseDouble(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Long)
                        affectedIndexes.get(j).put(row_id, Long.parseLong(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Date)
                        try {
                            affectedIndexes.get(j).put(row_id, _format.parse(valuesToIndex.get(j)));
                        } catch (final java.text.ParseException e) {
                            throw new RuntimeException(
                                    "Parsing problem in StormThetaJoin.updatedIndexes "
                                            + e.getMessage());
                        }
                    else if (typesOfValuesToIndex.get(j) instanceof String)
                        affectedIndexes.get(j).put(row_id, valuesToIndex.get(j));
                    else
                        throw new RuntimeException("non supported type");

                valuesToIndexMap.put(key, valuesToIndex);

            } else if (secondRelationIndexes.containsKey(keyReverse)) {
                List<Index> affectedIndexes = secondRelationIndexes.get(keyReverse);

                final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(false, tuple);
                Predicate _joinPredicate = joinPredicates.get(keyReverse);
                _joinPredicate.accept(visitor);

                final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
                        visitor._typesOfValuesToIndex);
                final List<String> valuesToIndex = new ArrayList<String>(
                        visitor._valuesToIndex);

                for (int j = 0; j < affectedIndexes.size(); j++)
                    if (typesOfValuesToIndex.get(j) instanceof Integer)
                        affectedIndexes.get(j).put(row_id, Integer.parseInt(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Double)
                        affectedIndexes.get(j).put(row_id, Double.parseDouble(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Long)
                        affectedIndexes.get(j).put(row_id, Long.parseLong(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Date)
                        try {
                            affectedIndexes.get(j).put(row_id,
                                    _format.parse(valuesToIndex.get(j)));
                        } catch (final java.text.ParseException e) {
                            throw new RuntimeException(
                                    "Parsing problem in StormThetaJoin.updatedIndexes "
                                            + e.getMessage());
                        }
                    else if (typesOfValuesToIndex.get(j) instanceof String)
                        affectedIndexes.get(j).put(row_id, valuesToIndex.get(j));
                    else
                        throw new RuntimeException("non supported type");

                valuesToIndexMap.put(keyReverse, valuesToIndex);
            }
        }
    }

    // TODO WINDOW Semantics
    @Override
    public void purgeStaleStateFromWindow() {
        System.gc();
    }

}

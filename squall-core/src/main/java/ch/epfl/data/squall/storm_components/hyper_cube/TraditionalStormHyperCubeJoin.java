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
import java.text.ParseException;
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


public class TraditionalStormHyperCubeJoin extends StormBoltComponent {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(TraditionalStormHyperCubeJoin.class);

    private ChainOperator operatorChain;
    private long numSentTuples = 0;
    private boolean firstTime = true;
    private PeriodicAggBatchSend periodicAggBatch;
    private long aggBatchOutputMillis;

    private ArrayList<TupleStorage> storages;
    private ArrayList<StormEmitter> emitters;
    private ArrayList<String> emitterIndexes;
    private ArrayList<String> emitterNames;

    // for fast acces Emitter name and Storage if we know emitter index.
    private HashMap<String, Integer> emitterIndexToEmitterName;
    private HashMap<String, Integer> emitterIndexToStorage;

    Map<String, Predicate> joinPredicates;
    // For each relation that has predicate between them we create Index.
    // firstRelationIndexes and secondRelationIndexes store Indexes between two relations.
    // [Key, ArrayList[Index]] -> key = [FirstRelation+SecondRelation], string concatenation of two relation names
    // Same for secondRelationIndexes, key = [SecondRelation+FirstRelation].
    // Look at createIndexes() function for more understanding
    private Map<String, ArrayList<Index>> firstRelationIndexes = new HashMap<String, ArrayList<Index>>();
    private Map<String, ArrayList<Index>> secondRelationIndexes = new HashMap<String, ArrayList<Index>>();

    private Map<String, ArrayList<Integer>> operatorForIndexes = new HashMap<String, ArrayList<Integer>>();
    private Map<String, ArrayList<Object>> typeOfValueIndexed = new HashMap<String, ArrayList<Object>>();
    protected boolean existIndexes = false;

    // for printing statistics for creating graphs
    protected Calendar cal = Calendar.getInstance();
    protected DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    protected SimpleDateFormat format = new SimpleDateFormat(
            "EEE MMM d HH:mm:ss zzz yyyy");
    protected DateFormat convDateFormat = new SimpleDateFormat(
            "EEE MMM d HH:mm:ss zzz yyyy");    

    public TraditionalStormHyperCubeJoin(ArrayList<StormEmitter> emitters, ComponentProperties cp,
                               List<String> allCompNames, Map<String, Predicate> joinPredicates, int hierarchyPosition,
                               TopologyBuilder builder, TopologyKiller killer, Config conf, Type wrapper) {

        super(cp, allCompNames, hierarchyPosition, false, conf);

        this.joinPredicates = joinPredicates;
        aggBatchOutputMillis = cp.getBatchOutputMillis();
        operatorChain = cp.getChainOperator();

        final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
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

        emitterIndexes = new ArrayList<String>();
        emitterNames = new ArrayList<String>();
        storages = new ArrayList<TupleStorage>();
        emitterIndexToEmitterName = new HashMap<String, Integer>();
        emitterIndexToStorage = new HashMap<String, Integer>();

        for (int i = 0; i < emitters.size(); i++) {
            String emitterIndex = String.valueOf(allCompNames.indexOf(emitters.get(i).getName()));
            emitterIndexes.add(emitterIndex);

            emitterNames.add(emitters.get(i).getName());

            storages.add(new TupleStorage());

            emitterIndexToEmitterName.put(emitterIndex, i);
            emitterIndexToStorage.put(emitterIndex, i);
        }

        if (joinPredicates != null) {
            createIndexes();
            existIndexes = true;
        } else {
            existIndexes = false;
        }    
    }

    // For each emitter we check is there Predicate between them.
    // If yes, we create Indexes between them and add to firstRelationIndexes and secondRelationIndexes.
    private void createIndexes() {
        for (int i = 0; i < emitterIndexes.size(); i++) {
            for (int j = i + 1; j < emitterIndexes.size(); j++) {
                String key = emitterNames.get(i) + emitterNames.get(j);
                String keyReverse = emitterNames.get(j) + emitterNames.get(i);

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

    // The same architecture that is in StormJoinBoltComponent.
    // However, we consider that there can be many different predicates for given relation
    // with different other relations. So, we should update all of them.
    private void updateIndexes(String inputComponentIndex, List<String> tuple, int row_id) {
        String emitterName = emitterNames.get(emitterIndexToEmitterName.get(inputComponentIndex));
        for (int i = 0; i < emitterIndexes.size(); i++) {
            String key = emitterName + emitterNames.get(i);
            String keyReverse = emitterNames.get(i) + emitterName;
            
            List<Index> affectedIndexes = null;
            PredicateUpdateIndexesVisitor visitor = null;
            Predicate joinPredicate = null;

            if (firstRelationIndexes.containsKey(key)) {
                affectedIndexes = firstRelationIndexes.get(key);
                visitor = new PredicateUpdateIndexesVisitor(true, tuple);
                joinPredicate = joinPredicates.get(key);
                joinPredicate.accept(visitor);

            } else if (firstRelationIndexes.containsKey(keyReverse)) {
                affectedIndexes = secondRelationIndexes.get(key);

                visitor = new PredicateUpdateIndexesVisitor(false, tuple);
                joinPredicate = joinPredicates.get(keyReverse);
                joinPredicate.accept(visitor);
            }

            if ((visitor != null) && (affectedIndexes != null)) {
                final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
                        visitor._typesOfValuesToIndex);
                final List<String> valuesToIndex = new ArrayList<String>(
                        visitor._valuesToIndex);

                for (int j = 0; j < affectedIndexes.size(); j++) {
                    if (typesOfValuesToIndex.get(j) instanceof Integer)
                        affectedIndexes.get(j).put(row_id, Integer.parseInt(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Double)
                        affectedIndexes.get(j).put(row_id, Double.parseDouble(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Long)
                        affectedIndexes.get(j).put(row_id, Long.parseLong(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Date)
                        try {
                            affectedIndexes.get(j).put(row_id,
                                    format.parse(valuesToIndex.get(j)));
                        } catch (final java.text.ParseException e) {
                            throw new RuntimeException(
                                    "Parsing problem in StormThetaJoin.updatedIndexes "
                                            + e.getMessage());
                        }
                    else if (typesOfValuesToIndex.get(j) instanceof String)
                        affectedIndexes.get(j).put(row_id, valuesToIndex.get(j));
                    else
                        throw new RuntimeException("non supported type");
                }
            }

        }
    }

    @Override
    public void aggBatchSend() {
    }

    @Override
    public void execute(Tuple stormTupleRcv) {
        if (firstTime
            && MyUtilities.isAggBatchOutputMode(aggBatchOutputMillis)) {
            periodicAggBatch = new PeriodicAggBatchSend(aggBatchOutputMillis,
                this);
            firstTime = false;
        }

        if (receivedDumpSignal(stormTupleRcv)) {
            MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
            return;
        }

        if (!MyUtilities.isManualBatchingMode(getConf())) {
            final String inputComponentIndex = stormTupleRcv
                .getStringByField(StormComponent.COMP_INDEX); // getString(0);
            final List<String> tuple = (List<String>) stormTupleRcv
                .getValueByField(StormComponent.TUPLE); // getValue(1);
            final String inputTupleHash = stormTupleRcv
                .getStringByField(StormComponent.HASH);// getString(2);
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
            final String[] parts = currentTuple
                .split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
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
                processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, true);
            }        
        }
    }

    private void processNonLastTuple(String inputComponentIndex,
                                     String inputTupleString, //
                                     List<String> tuple, // these two are the same
                                     Tuple stormTupleRcv, boolean isLastInBatch) {

        TupleStorage affectedStorage = storages.get(emitterIndexToStorage.get(inputComponentIndex));
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

    protected void performJoin(Tuple stormTupleRcv, List<String> tuple, int rowID,
                               String emitterIndex, boolean isLastInBatch) {
        
        boolean[] visited = new boolean[storages.size()];
        ArrayList<int[]> outputTuples = new ArrayList<>();

        int storageIndex = getStorageIndex(emitterIndex);

        int[] firstResult = new int[storages.size()];
        Arrays.fill(firstResult, -1);
        firstResult[storageIndex] = rowID;

        outputTuples.add(firstResult);

        visit(storageIndex, visited, 1, outputTuples);
    }

    public void visit(int storageIndex, boolean[] visited, 
        int numberOfJoinedRelations, ArrayList<int[]> outputTuples) {
        visited[storageIndex] = true;

        // return results
        if (numberOfJoinedRelations == visited.length) {
            for (int[] tuplesID : outputTuples) {
                List<String> result = new ArrayList<String>();
                
                for (int i = 0; i < tuplesID.length; i++) {
                    result.add(storages.get(i).get(tuplesID[i]));
                }

                // output tuple.
            }
        }

        // we have already visited some of realtion and the last one is storageIndex
        // from all visited relations we need check what we predicate we have with unvisited relations
        // and choose the relation with lowest number of tuples - trying to choose low selectivity
        int nextEmitterIndex = -1;
        int lowSelectivity = Integer.MAX_VALUE;

        for (int i = 0; i < visited.length; i++) {
            
            if (!visited[i]) {
                continue;
            }

            for (int j = 0; j < visited.length; j++) {
                
                if (visited[j]) {
                    continue;
                }

                String key = emitterNames.get(i) + emitterNames.get(j);
                String keyReverse = emitterNames.get(j) + emitterNames.get(i);

                if (joinPredicates.containsKey(key) && storages.get(j).size() < lowSelectivity) {
                    lowSelectivity = storages.get(j).size();
                    nextEmitterIndex = j;
                }

                if (joinPredicates.containsKey(keyReverse) && storages.get(j).size() < lowSelectivity) {
                    lowSelectivity = storages.get(j).size();
                    nextEmitterIndex = j;
                }

            }
        }

        // select tuples for given relationship
        ArrayList<int[]> newOutputTuples = new ArrayList<>();

        for (int[] tuplesID : outputTuples) {
            TIntArrayList resultID = new TIntArrayList();
            boolean firstTime = true;

            for (int i = 0; i < tuplesID.length; i++) {
                if (tuplesID[i] != -1) {
                    String key = emitterNames.get(nextEmitterIndex) + emitterNames.get(i);
                    String keyReverse = emitterNames.get(i) + emitterNames.get(nextEmitterIndex);
                    
                    TIntArrayList rowIds = new TIntArrayList();

                    if (joinPredicates.containsKey(key)) {
                        rowIds = selectTupleToJoin(
                                    key,
                                    storages.get(nextEmitterIndex), 
                                    firstRelationIndexes.get(key),
                                    true,
                                    MyUtilities.stringToTuple(storages.get(i).get(tuplesID[i]), getConf()));
                    } else if (joinPredicates.containsKey(keyReverse)) {
                        rowIds = selectTupleToJoin(
                                    keyReverse,
                                    storages.get(nextEmitterIndex), 
                                    secondRelationIndexes.get(key),
                                    false,
                                    MyUtilities.stringToTuple(storages.get(i).get(tuplesID[i]), getConf()));
                    }

                    if (firstTime) {
                        firstTime = false;
                        resultID.addAll(rowIds);
                    } else {
                        resultID.retainAll(rowIds);
                    }
                }
            }

            for (int i = 0; i < resultID.size(); i++) {
                final int id = resultID.get(i);
                int[] newJoin = new int[tuplesID.length];
                for (int j = 0; j < tuplesID.length; j++) {
                    newJoin[j] = tuplesID[j];
                }

                newJoin[storageIndex] = id;
                newOutputTuples.add(newJoin);
            }
        }

        visit(nextEmitterIndex, visited, numberOfJoinedRelations + 1, newOutputTuples);
    }

    // Specific to TupleStorage
    protected TIntArrayList selectTupleToJoin(String key, TupleStorage oppositeStorage,
        List<Index> oppositeIndexes, boolean isFromFirstEmitter,
        List<String> valuesToApplyOnIndex) {
        
        final TIntArrayList rowIds = new TIntArrayList();

        for (int i = 0; i < oppositeIndexes.size(); i++) {
            TIntArrayList currentRowIds = null;
            final Index currentOpposIndex = oppositeIndexes.get(i);
            final String value = valuesToApplyOnIndex.get(i);
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
                currentRowIds = currentOpposIndex.getValues(currentOperator, value);
            // Even if valueIndexed is at first time an integer with
            // precomputation a*col +b, it become a double
            else if (typeOfValueIndexed.get(key).get(i) instanceof Integer)
                currentRowIds = currentOpposIndex.getValues(currentOperator, Integer.parseInt(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Long)
                currentRowIds = currentOpposIndex.getValues(currentOperator, Long.parseLong(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Double)
                currentRowIds = currentOpposIndex.getValues(currentOperator, Double.parseDouble(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Date)
                try {
                    currentRowIds = currentOpposIndex.getValues(
                        currentOperator, convDateFormat.parse(value));
                } catch (final ParseException e) {
                    e.printStackTrace();
                }
            else
                throw new RuntimeException("non supported type");
            if (currentRowIds == null)
                return new TIntArrayList();
            if (i == 0)
                rowIds.addAll(currentRowIds);
            else
                rowIds.retainAll(currentRowIds);
            if (rowIds.isEmpty())
                return new TIntArrayList();;
        }

        return rowIds;
    }


    public void printStoragaSize() {
        LOG.info("*******************************************************");
        for (TupleStorage ts : storages) {
            LOG.info("The size is : " + ts.size());
        }
        LOG.info("*******************************************************");
    }

    public int getStorageIndex(String emitterIndex) {
        for (int i = 0; i < emitterIndexes.size(); i++) {
            if (emitterIndexes.get(i).equals(emitterIndex)) {
                return i;
            }
        }
        return -1;
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
        return periodicAggBatch;
    }

    @Override
    protected void printStatistics(int type) {
    }

    // TODO WINDOW Semantics
    @Override
    public void purgeStaleStateFromWindow() {
        System.gc();
    }

}

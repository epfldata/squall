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

import ch.epfl.data.squall.storm_components.hash_hypercube.HashHyperCubeGrouping.EmitterDesc;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignmentBruteForce;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignmentBruteForce.ColumnDesc;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignerFactory;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment.Dimension;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HybridHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HybridHyperCubeAssignmentBruteForce;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.PartitioningScheme;

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

    private long _numInputTuples = 0;
    protected StatisticsUtilities _statsUtils;

    private ChainOperator operatorChain;
    private long numSentTuples = 0;
    private boolean firstTime = true;
    private PeriodicAggBatchSend periodicAggBatch;
    private long aggBatchOutputMillis;

    private ArrayList<TupleStorage> storages;
    private ArrayList<StormEmitter> emitters;
    private ArrayList<String> emitterIndexes;
    private ArrayList<String> emitterNames;

    private Map<String, String[]> _emitterColNames;
    private Map<String, Type[]> _emitterNamesColTypes;
    private Set<String> _randomColumns;

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

    public TraditionalStormHyperCubeJoin(ArrayList<StormEmitter> emitters, Map<String, String[]> emitterColNames,
                                Map<String, Type[]> emitterNamesColTypes, Set<String> randomColumns,
                                ComponentProperties cp, List<String> allCompNames, Map<String, Predicate> joinPredicates, 
                                int hierarchyPosition, TopologyBuilder builder, TopologyKiller killer, 
                                Config conf, Type wrapper) {

        super(cp, allCompNames, hierarchyPosition, false, conf);

        _statsUtils = new StatisticsUtilities(getConf(), LOG);

        _emitterColNames = emitterColNames;
        _emitterNamesColTypes = emitterNamesColTypes;
        _randomColumns = randomColumns;

        this.emitters = emitters;

        this.joinPredicates = joinPredicates;
        aggBatchOutputMillis = cp.getBatchOutputMillis();
        operatorChain = cp.getChainOperator();

        final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

        
        currentBolt = attachEmitters(conf, currentBolt, allCompNames, parallelism);

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
            for (int j = 0; j < emitterIndexes.size(); j++) {
                
                String key = emitterNames.get(i) + emitterNames.get(j);

                if (joinPredicates.containsKey(key)) {
                    Predicate pr = joinPredicates.get(key);
                  
                    final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
                    pr.accept(visitor);
                  
                    firstRelationIndexes.put(key, new ArrayList<Index>(visitor._firstRelationIndexes));
                    secondRelationIndexes.put(key, new ArrayList<Index>(visitor._secondRelationIndexes));

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
            String keyAB = emitterName + emitterNames.get(i);
            String keyBA = emitterNames.get(i) + emitterName;
            
            List<Index> affectedIndexes = null;
            PredicateUpdateIndexesVisitor visitor = null;
            Predicate joinPredicate = null;

            if (firstRelationIndexes.containsKey(keyAB)) {
                affectedIndexes = firstRelationIndexes.get(keyAB);
                visitor = new PredicateUpdateIndexesVisitor(true, tuple);
                joinPredicate = joinPredicates.get(keyAB);
                joinPredicate.accept(visitor);

            } else if (firstRelationIndexes.containsKey(keyBA)) {
                affectedIndexes = secondRelationIndexes.get(keyBA);

                visitor = new PredicateUpdateIndexesVisitor(false, tuple);
                joinPredicate = joinPredicates.get(keyBA);
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


    protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> inTuple, long lineageTimestamp) {
        //LOG.info(inTuple);

        for (List<String> tuple : operatorChain.process(inTuple, lineageTimestamp)) {
          if (tuple == null)
            return;
          
          numSentTuples++;
          printTuple(tuple);
          //LOG.info(tuple);
          
          if (numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
            printStatistics(SystemParameters.OUTPUT_PRINT);

          if (MyUtilities.isSending(getHierarchyPosition(), aggBatchOutputMillis)) {
            tupleSend(tuple, stormTupleRcv, lineageTimestamp);
          }
        }
    }


    @Override
    public void aggBatchSend() {
    }

   private InputDeclarer attachEmitters(Config conf, InputDeclarer currentBolt,
                 List<String> allCompNames, int parallelism) {

        switch (getPartitioningScheme(conf)) {
            case BRUTEFORCEHYBRIDHYPERCUBE:
                long[] cardinality = getEmittersCardinality(emitters, conf);
                List<ColumnDesc> columns = getColumnDesc(cardinality, emitters);

                List<EmitterDesc> emittersDesc = MyUtilities.getEmitterDesc(
                        emitters, _emitterColNames, allCompNames, cardinality);

                LOG.info("cardinalities: " + Arrays.toString(cardinality));

                HybridHyperCubeAssignment _currentHybridHyperCubeMappingAssignment = 
                    new HybridHyperCubeAssignmentBruteForce(emittersDesc, columns, _randomColumns, parallelism);
                
                LOG.info("assignment: " + _currentHybridHyperCubeMappingAssignment.getMappingDimensions());

                currentBolt = MyUtilities.attachEmitterHybridHyperCube(currentBolt, 
                        emitters, _emitterColNames, allCompNames,
                        _currentHybridHyperCubeMappingAssignment, emittersDesc, conf);   
                break;
            case HASHHYPERCUBE:
                cardinality = getEmittersCardinality(emitters, conf);
                LOG.info("cardinalities: " + Arrays.toString(cardinality));
                
                columns = getColumnDesc(cardinality, emitters);
                emittersDesc = MyUtilities.getEmitterDesc(
                        emitters, _emitterColNames, allCompNames, cardinality);

                HashHyperCubeAssignment _currentHashHyperCubeMappingAssignment = 
                    new HashHyperCubeAssignmentBruteForce(parallelism, columns, emittersDesc);
                
                LOG.info("assignment: " + _currentHashHyperCubeMappingAssignment.getMappingDimensions());

                currentBolt = MyUtilities.attachEmitterHashHyperCube(currentBolt, 
                        emitters, _emitterColNames, _currentHashHyperCubeMappingAssignment, 
                        emittersDesc, conf);
                break;            
            case HYPERCUBE:
                cardinality = getEmittersCardinality(emitters, conf);
                LOG.info("cardinalities: " + Arrays.toString(cardinality));
                final HyperCubeAssignment _currentHyperCubeMappingAssignment =
                        new HyperCubeAssignerFactory().getAssigner(parallelism, cardinality);

                LOG.info("assignment: " + _currentHyperCubeMappingAssignment.getMappingDimensions());
                currentBolt = MyUtilities.attachEmitterHyperCube(currentBolt,
                        emitters, allCompNames,
                        _currentHyperCubeMappingAssignment, conf);
                break;
        }
        return currentBolt;
    }

    private List<ColumnDesc> getColumnDesc(long[] cardinality, List<StormEmitter> emitters) {
        HashMap<String, ColumnDesc> tmp = new HashMap<String, ColumnDesc>();

        List<ColumnDesc> desc = new ArrayList<ColumnDesc>();
        
        for (int i = 0; i < emitters.size(); i++) {
            String emitterName = emitters.get(i).getName();
            Type[] columnTypes = _emitterNamesColTypes.get(emitterName);
            String[] columnNames = _emitterColNames.get(emitterName);

            for (int j = 0; j < columnNames.length; j++) {
                ColumnDesc cd = new ColumnDesc(columnNames[j], columnTypes[j], cardinality[i]);

                if (tmp.containsKey(cd.name)) {
                    cd.size += tmp.get(cd.name).size;
                    tmp.put(cd.name, cd);
                } else {
                    tmp.put(cd.name, cd);
                }
            }
        }

        for (String key : tmp.keySet()) {
            desc.add(tmp.get(key));
        }

        return desc;
    }

    private PartitioningScheme getPartitioningScheme(Config conf) {
        String schemeName = SystemParameters.getString(conf, getName() + "_PART_SCHEME");
        if (schemeName == null || schemeName.equals("")) {
            LOG.info("use default Hypercube partitioning scheme");
            return PartitioningScheme.HYPERCUBE;
        } else {
            LOG.info("use partitioning scheme : " + schemeName);
            return PartitioningScheme.valueOf(schemeName);
        }
    }

    private long[] getEmittersCardinality(List<StormEmitter> emitters, Config conf) {
        long[] cardinality = new long[emitters.size()];
        for (int i = 0; i < emitters.size(); i++) {
            cardinality[i] = SystemParameters.getInt(conf, emitters.get(i).getName() + "_CARD");
        }
        return cardinality;
    }

    @Override
    public void execute(Tuple stormTupleRcv) {
        if (receivedDumpSignal(stormTupleRcv)) {
            MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
            return;
        }

        final String inputComponentIndex = stormTupleRcv.getStringByField(StormComponent.COMP_INDEX); // getString(0);
        final List<String> tuple = (List<String>) stormTupleRcv.getValueByField(StormComponent.TUPLE); // getValue(1);
        final String inputTupleHash = stormTupleRcv.getStringByField(StormComponent.HASH);// getString(2);
        
        if (processFinalAck(tuple, stormTupleRcv))
            return;
            
        final String inputTupleString = MyUtilities.tupleToString(tuple, getConf());
        processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, true);
    
        getCollector().ack(stormTupleRcv);
    }

    private void processNonLastTuple(String inputComponentIndex,
                                     String inputTupleString, //
                                     List<String> tuple, // these two are the same
                                     Tuple stormTupleRcv, boolean isLastInBatch) {

        _numInputTuples++;

        TupleStorage affectedStorage = storages.get(emitterIndexToStorage.get(inputComponentIndex));
        // add the stormTuple to the specific storage

        final int row_id = affectedStorage.insert(inputTupleString);
        if (existIndexes)
            updateIndexes(inputComponentIndex, tuple, row_id);

        performJoin(stormTupleRcv, tuple, row_id, inputComponentIndex, isLastInBatch);

        if (_numInputTuples % _statsUtils.getDipInputFreqPrint() == 0)
            printStatistics(SystemParameters.INPUT_PRINT);      
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

        visit(stormTupleRcv, storageIndex, visited, 1, outputTuples);
    }

    public void visit(Tuple stormTupleRcv, int storageIndex, boolean[] visited, 
        int numberOfJoinedRelations, ArrayList<int[]> outputTuples) {
        visited[storageIndex] = true;

        // LOG.info("****************************");
        // LOG.info(emitterNames);
        // LOG.info(Arrays.toString(visited));
        // for (int[] tuplesID : outputTuples)
        // {
        //     LOG.info(Arrays.toString(tuplesID));
        // }
        // LOG.info("****************************");

        // return results
        if (numberOfJoinedRelations == visited.length) {
            for (int[] tuplesID : outputTuples) {
                List<List<String>> result = new ArrayList<>();
                boolean foundMissing = false;
                for (int i = 0; i < tuplesID.length; i++) {
                    if (tuplesID[i] < 0) {
                        foundMissing = true;
                        break;
                    }

                    result.add(MyUtilities.stringToTuple(storages.get(i).get(tuplesID[i]), getConf()));
                }

                long lineageTimestamp = 0L;
                if (!foundMissing)
                    applyOperatorsAndSend(stormTupleRcv, MyUtilities.createOutputTuple(result), lineageTimestamp);
            }

            return;
        }

        // we have already visited some of realtion and the last one is storageIndex
        // from all visited relations we need check we predicate we have with unvisited relations
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

                String keyAB = emitterNames.get(i) + emitterNames.get(j);
                String keyBA = emitterNames.get(j) + emitterNames.get(i);

                //LOG.info("*********** " + keyAB + " - " + keyBA + " ***********");

                if (joinPredicates.containsKey(keyAB) && storages.get(j).size() < lowSelectivity) {
                    lowSelectivity = storages.get(j).size();
                    nextEmitterIndex = j;
                }

                if (joinPredicates.containsKey(keyBA) && storages.get(j).size() < lowSelectivity) {
                    lowSelectivity = storages.get(j).size();
                    nextEmitterIndex = j;
                }

            }
        }

        //LOG.info("Emitter Index : " + nextEmitterIndex + " , selectivity : " + lowSelectivity);

        // if (nextEmitterIndex == -1)
        //     return;

        // select tuples for given relationship
        ArrayList<int[]> newOutputTuples = new ArrayList<>();

        for (int[] tuplesID : outputTuples) {
            TIntArrayList resultID = new TIntArrayList();
            boolean firstTime = true;

            for (int i = 0; i < tuplesID.length; i++) {
                // for not visited we would have -1
                if (tuplesID[i] != -1) {
                    String keyAB = emitterNames.get(nextEmitterIndex) + emitterNames.get(i);
                    String keyBA = emitterNames.get(i) + emitterNames.get(nextEmitterIndex);
                    
                    TIntArrayList rowIds = new TIntArrayList();
                    boolean isFromFirstEmitter;
                    boolean hasJoinPredicate = false;

                    if (joinPredicates.containsKey(keyAB)) {
                        hasJoinPredicate = true;
                        isFromFirstEmitter = false;
                        final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
                            isFromFirstEmitter, MyUtilities.stringToTuple(storages.get(i).get(tuplesID[i]), getConf()));
                        
                        joinPredicates.get(keyAB).accept(visitor);
                        final List<String> valuesToIndex = new ArrayList<String>(visitor._valuesToIndex);
                        
                        rowIds = selectTupleToJoin(
                                    keyAB,
                                    storages.get(nextEmitterIndex), 
                                    firstRelationIndexes.get(keyAB),
                                    isFromFirstEmitter,
                                    valuesToIndex);

                    } else if (joinPredicates.containsKey(keyBA)) {
                        hasJoinPredicate = true;
                        isFromFirstEmitter = true;
                        final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
                            isFromFirstEmitter, MyUtilities.stringToTuple(storages.get(i).get(tuplesID[i]), getConf()));
                        
                        joinPredicates.get(keyBA).accept(visitor);
                        final List<String> valuesToIndex = new ArrayList<String>(visitor._valuesToIndex);
                                                
                        rowIds = selectTupleToJoin(
                                    keyBA,
                                    storages.get(nextEmitterIndex), 
                                    secondRelationIndexes.get(keyBA),
                                    isFromFirstEmitter,
                                    valuesToIndex);
                    }
    
                    if (hasJoinPredicate) {
                        //LOG.info("RESULT IS : " + resultID);

                        if (firstTime) {
                            firstTime = false;
                            resultID.addAll(rowIds);
                        } else {
                            resultID.retainAll(rowIds);
                        }
                        //LOG.info("ROW IDS : " + rowIds);
                        //LOG.info("RESULT IS : " + resultID);
                    }
                }
            }

            for (int i = 0; i < resultID.size(); i++) {
                final int id = resultID.get(i);
                int[] newJoin = new int[tuplesID.length];
                for (int j = 0; j < tuplesID.length; j++) {
                    newJoin[j] = tuplesID[j];
                }

                newJoin[nextEmitterIndex] = id;
                newOutputTuples.add(newJoin);
            }
        }

        visit(stormTupleRcv, nextEmitterIndex, visited, numberOfJoinedRelations + 1, newOutputTuples);
    }

    // Specific to TupleStorage
    protected TIntArrayList selectTupleToJoin(String key, TupleStorage oppositeStorage,
        List<Index> oppositeIndexes, boolean isFromFirstEmitter,
        List<String> valuesToApplyOnIndex) {

        // LOG.info(key);
        // LOG.info(valuesToApplyOnIndex);

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
            else if (typeOfValueIndexed.get(key).get(i) instanceof Integer) {
                currentRowIds = currentOpposIndex.getValues(currentOperator, Integer.parseInt(value));
            }
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

        // LOG.info(rowIds);
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
        if (type == SystemParameters.INPUT_PRINT)
            LOG.info(", Executed Input Tuples : " + _numInputTuples);
    }

    // TODO WINDOW Semantics
    @Override
    public void purgeStaleStateFromWindow() {
        System.gc();
    }

}

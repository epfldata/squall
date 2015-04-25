/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.storm_components.dbtoaster;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.dbtoaster.DBToasterEngine;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormBoltComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.PartitioningScheme;
import ch.epfl.data.squall.utilities.PeriodicAggBatchSend;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.statistics.StatisticsUtilities;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class StormDBToasterJoin extends StormBoltComponent {

    private static Logger LOG = Logger.getLogger(StormDBToasterJoin.class);

    private final ChainOperator _operatorChain;

    private long _numSentTuples = 0;

    // for load-balancing
    private final List<String> _fullHashList;

    // for batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private final long _aggBatchOutputMillis;

    // for printing statistics for creating graphs
    protected Calendar _cal = Calendar.getInstance();
    protected DateFormat _statDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    protected StatisticsUtilities _statsUtils;

    private DBToasterEngine dbtoasterEngine;
    private static final String DBT_GEN_PKG = "ddbt.gen.";
    private String _dbToasterQueryName;

    private StormEmitter[] _emitters;
    private Map<String, ValueExpression[]> _indexedColRefs;

    public StormDBToasterJoin(StormEmitter[] emitters,
                              ComponentProperties cp, List<String> allCompNames,
                              Map<String, ValueExpression[]> emitterNameColRefs,
                              PartitioningScheme partitioningScheme,
                              int hierarchyPosition, TopologyBuilder builder,
                              TopologyKiller killer, Config conf) {
        super(cp, allCompNames, hierarchyPosition, conf);


        _emitters = emitters;
        _indexedColRefs = new HashMap<String, ValueExpression[]>();
        for (StormEmitter e : _emitters) {
            String emitterIndex = String.valueOf(allCompNames.indexOf(e.getName()));
            ValueExpression[] colRefs = emitterNameColRefs.get(e.getName());
            _indexedColRefs.put(emitterIndex, colRefs);
        }

        _operatorChain = cp.getChainOperator();
        _fullHashList = cp.getFullHashList();

        _dbToasterQueryName = cp.getName() + "Impl";

        _aggBatchOutputMillis = cp.getBatchOutputMillis();

        _statsUtils = new StatisticsUtilities(getConf(), LOG);

        final int parallelism = SystemParameters.getInt(getConf(), getID()
                + "_PAR");

        // connecting with previous level using Hypercube Assignment
        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
        currentBolt = attachEmitters(conf, currentBolt, partitioningScheme);


        // connecting with Killer
        if (getHierarchyPosition() == FINAL_COMPONENT
                && (!MyUtilities.isAckEveryTuple(conf)))
            killer.registerComponent(this, parallelism);
        if (cp.getPrintOut() && _operatorChain.isBlocking())
            currentBolt.allGrouping(killer.getID(),
                    SystemParameters.DUMP_RESULTS_STREAM);
    }

    private InputDeclarer attachEmitters(Config conf, InputDeclarer currentBolt,
                 List<String> allCompNames, PartitioningScheme partitioningScheme) {
        switch (partitioningScheme) {
            case HYPERCUBE:
                throw new RuntimeException("Hypercube partitioning is not yet supported");
            case STARSCHEMA:
                long[] cardinality = getEmittersCardinality(conf);
                currentBolt = MyUtilities.attachEmitterStarSchema(conf,
                        currentBolt, _emitters, allCompNames, cardinality);
            case HASH:
                currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList,
                    currentBolt, _emitters[0], Arrays.copyOfRange(_emitters, 1, _emitters.length));
                break;
        }
        return currentBolt;
    }

    private long[] getEmittersCardinality(Config conf) {
        long[] cardinality = new long[_emitters.length];
        for (int i = 0; i < _emitters.length; i++) {
            cardinality[i] = SystemParameters.getInt(conf, _emitters[i].getName() + "_CARD");
        }
        return cardinality;
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
        super.prepare(map, tc, collector);

        dbtoasterEngine = new DBToasterEngine(DBT_GEN_PKG + _dbToasterQueryName);
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
            final List<String> tuple = (List<String>) stormTupleRcv
                    .getValueByField(StormComponent.TUPLE); // getValue(1);

            if (processFinalAck(tuple, stormTupleRcv)) {
                // need to close db toaster app here
                dbtoasterEngine.endStream();
                return;
            }

            processNonLastTuple(inputComponentIndex, tuple,
                    stormTupleRcv, true);
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
                String inputTupleString = null;
                if (parts.length == 1)
                    // lastAck
                    inputTupleString = new String(parts[0]);
                else {
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
                processNonLastTuple(inputComponentIndex, tuple,
                        stormTupleRcv, (i == batchSize - 1));

            }
        }

        getCollector().ack(stormTupleRcv);
    }

    private void processNonLastTuple(String inputComponentIndex,
                                     List<String> tuple, Tuple stormTupleRcv,
                                     boolean isLastInBatch) {

        ValueExpression[] colRefs = _indexedColRefs.get(inputComponentIndex);
        performJoin(stormTupleRcv, tuple, colRefs, isLastInBatch);

    }

    protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
                               ValueExpression[] columnReferences,
                               boolean isLastInBatch) {

        List<Object> typedTuple = createTypedTuple(tuple, columnReferences);

        dbtoasterEngine.insertTuple(stormTupleRcv.getSourceComponent(), typedTuple.toArray());

        List<Object[]> stream = dbtoasterEngine.getStreamOfUpdateTuples();

        long lineageTimestamp = 0L;
        if (MyUtilities.isCustomTimestampMode(getConf()))
            lineageTimestamp = stormTupleRcv
                    .getLongByField(StormComponent.TIMESTAMP);

        for (Object[] u : stream) {
            List<String> outputTuple = createStringTuple(u);
            applyOperatorsAndSend(stormTupleRcv, outputTuple, lineageTimestamp, isLastInBatch);
        }

    }

    private List<Object> createTypedTuple(List<String> tuple, ValueExpression[] columnReferences) {
        List<Object> typedTuple = new ArrayList<Object>();

        for (int i = 0; i < columnReferences.length; i++) {
            ValueExpression ve = columnReferences[i];
            Object value = ve.eval(tuple);
            typedTuple.add(value);
        }
        return typedTuple;
    }

    private List<String> createStringTuple(Object[] typedTuple) {
        List<String> tuple = new LinkedList<String>();
        for (Object o : typedTuple) tuple.add("" + o);
        return tuple;
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv,
                                         List<String> tuple, long lineageTimestamp, boolean isLastInBatch) {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            try {
                _semAgg.acquire();
            } catch (final InterruptedException ex) {
            }

        tuple = _operatorChain.process(tuple, lineageTimestamp);

        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            _semAgg.release();
        if (tuple == null)
            return;
        _numSentTuples++;
        printTuple(tuple);

        if (_numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
            printStatistics(SystemParameters.OUTPUT_PRINT);

        if (MyUtilities
                .isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
            tupleSend(tuple, stormTupleRcv, lineageTimestamp);
        }

        if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
            if (!MyUtilities.isManualBatchingMode(getConf()) || isLastInBatch) {
                printTupleLatency(_numSentTuples - 1, lineageTimestamp);
            }
        }
    }

    @Override
    public ChainOperator getChainOperator() {
        return _operatorChain;
    }

    @Override
    protected InterchangingComponent getInterComp() {
        return null;
    }

    @Override
    public long getNumSentTuples() {
        return _numSentTuples;
    }

    @Override
    public PeriodicAggBatchSend getPeriodicAggBatch() {
        return _periodicAggBatch;
    }

    @Override
    protected void printStatistics(int type) {

    }

    @Override
    public void purgeStaleStateFromWindow() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void aggBatchSend() {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            if (_operatorChain != null) {
                final Operator lastOperator = _operatorChain.getLastOperator();
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

    @Override
    public String getInfoID() {
        return getID();
    }
}

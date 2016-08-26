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

package ch.epfl.data.squall.connectors.hdfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;

public class HDFSmaterializer implements Component {

    private final String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private final ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet;

    private final Component _parent;
    private Component _child;
    // private StormOperator _stormOperator;

    private List<String> _fullHashList;

    private String _hdfsPath;
    private String _folderName;
    private int _parallelism;

    public HDFSmaterializer(Component parent, String componentName,
	    String hdfsPath, String folderName, int parallelism) {

	_parent = parent;
	_parent.setChild(this);
	_componentName = componentName;
	_hdfsPath = hdfsPath;
	_folderName = folderName;
	_parallelism = parallelism;
    }

    public BaseRichBolt createHDFSmaterializer(String hdfsPath) {
	RecordFormat format = new DelimitedRecordFormat()
		.withFieldDelimiter("|");

	// sync the filesystem after every 1k tuples
	SyncPolicy syncPolicy = new CountSyncPolicy(1000);

	// rotate files when they reach 5MB
	FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f,
		Units.MB);

	FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
		"/" + _folderName + "/").withExtension(".txt");

	HdfsBolt bolt = new HdfsBolt().withFsUrl(hdfsPath)
		.withFileNameFormat(fileNameFormat).withRecordFormat(format)
		.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
	return bolt;
    }

    @Override
    public HDFSmaterializer add(Operator operator) {
	_chain.addOperator(operator);
	return this;
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof Component)
	    return _componentName.equals(((Component) obj).getName());
	else
	    return false;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources() {
	final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
	list.addAll(_parent.getAncestorDataSources());
	return list;
    }

    @Override
    public long getBatchOutputMillis() {
	return _batchOutputMillis;
    }

    @Override
    public ChainOperator getChainOperator() {
	return _chain;
    }

    @Override
    public Component getChild() {
	return _child;
    }

    // from StormComponent
    @Override
    public String[] getEmitterIDs() {
	throw new RuntimeException("Not implemented yet");
    }

    @Override
    public List<String> getFullHashList() {
	return _fullHashList;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
	return _hashExpressions;
    }

    @Override
    public List<Integer> getHashIndexes() {
	return _hashIndexes;
    }

    @Override
    public String getInfoID() {
	throw new RuntimeException("Not implemented yet");
    }

    @Override
    public String getName() {
	return _componentName;
    }

    @Override
    public Component[] getParents() {
	return new Component[] { _parent };
    }

    @Override
    public boolean getPrintOut() {
	return _printOut;
    }

    @Override
    public int hashCode() {
	int hash = 5;
	hash = 47 * hash
		+ (_componentName != null ? _componentName.hashCode() : 0);
	return hash;
    }

    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
	    List<String> allCompNames, Config conf, int hierarchyPosition) {

	// by default print out for the last component
	// for other conditions, can be set via setPrintOut
	if (hierarchyPosition == StormComponent.FINAL_COMPONENT
		&& !_printOutSet)
	    setPrintOut(true);

	MyUtilities.checkBatchOutput(_batchOutputMillis,
		_chain.getAggregation(), conf);

	BaseRichBolt hdfsBolt = createHDFSmaterializer(_hdfsPath);

	builder.setBolt("hdfs", hdfsBolt, _parallelism).shuffleGrouping(
		((StormEmitter) _parent).getEmitterIDs()[0]);

    }

    @Override
    public HDFSmaterializer setBatchOutputMillis(long millis) {
	_batchOutputMillis = millis;
	return this;
    }

    @Override
    public void setChild(Component child) {
	_child = child;
    }

    @Override
    public Component setContentSensitiveThetaJoinWrapper(Type wrapper) {
	return this;
    }

    @Override
    public HDFSmaterializer setFullHashList(List<String> fullHashList) {
	_fullHashList = fullHashList;
	return this;
    }

    @Override
    public HDFSmaterializer setHashExpressions(
	    List<ValueExpression> hashExpressions) {
	_hashExpressions = hashExpressions;
	return this;
    }

    @Override
    public HDFSmaterializer setOutputPartKey(int... hashIndexes) {
	return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public HDFSmaterializer setOutputPartKey(List<Integer> hashIndexes) {
	_hashIndexes = hashIndexes;
	return this;
    }

    @Override
    public HDFSmaterializer setPrintOut(boolean printOut) {
	_printOutSet = true;
	_printOut = printOut;
	return this;
    }

}

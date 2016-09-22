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

package ch.epfl.data.squall.components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormDataSource;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.ReaderProvider;
import ch.epfl.data.squall.utilities.FileReaderProvider;
import ch.epfl.data.squall.utilities.SystemParameters;

public class DataSourceComponent extends AbstractComponent<DataSourceComponent> {
    protected DataSourceComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DataSourceComponent.class);

    private final ReaderProvider _provider;
    private final String _resourceName;

    // equi-weight histogram
    private boolean _isPartitioner;

    // invoked from the new Interface (QueryPlan not QueryBuilder)
    //   be careful if there are aliases of the same table
    public DataSourceComponent(String tableName, Map conf) {
	this(tableName.toUpperCase(),
           // dataPath + tableName + extension);
           new FileReaderProvider(SystemParameters.getString(conf, "DIP_DATA_PATH")),
           tableName + SystemParameters.getString(conf, "DIP_EXTENSION"));
    }

    public DataSourceComponent(String componentName, String inputPath) {
	this(componentName, new FileReaderProvider(""), inputPath);
    }

    public DataSourceComponent(String componentName, ReaderProvider provider, String resourceName) {
        super((Component[])null, componentName);
        _provider = provider;
        _resourceName = resourceName;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources() {
	final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
	list.add(this);
	return list;
    }

    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
	    List<String> allCompNames, Config conf, int hierarchyPosition) {

	// by default print out for the last component
	// for other conditions, can be set via setPrintOut
	if (hierarchyPosition == StormComponent.FINAL_COMPONENT
		&& !getPrintOutSet())
	    setPrintOut(true);

	final int parallelism = SystemParameters.getInt(conf, getName()
		+ "_PAR");
	if (parallelism > 1 && getChainOperator().getDistinct() != null)
	    throw new RuntimeException(
                                       getName()
			    + ": Distinct operator cannot be specified for multiple spouts for one input file!");

	MyUtilities.checkBatchOutput(getBatchOutputMillis(),
                                     getChainOperator().getAggregation(), conf);

	setStormEmitter(new StormDataSource(this, allCompNames, _provider, _resourceName,
                                            hierarchyPosition, parallelism, _isPartitioner, builder,
                                            killer, conf));
    }

    @Override
    public DataSourceComponent setBatchOutputMillis(long millis) {
	throw new RuntimeException(
		"Setting batch mode is not allowed for DataSourceComponents!");
	// _batchOutputMillis = millis;
	// return this;
    }

    @Override
    public DataSourceComponent setFullHashList(List<String> fullHashList) {
	throw new RuntimeException(
		"This method should not be invoked for DataSourceComponent!");
    }

    @Override
    public List<String> getFullHashList() {
	throw new RuntimeException(
		"This method should not be invoked for DataSourceComponent!");
    }

    public DataSourceComponent setPartitioner(boolean isPartitioner) {
	_isPartitioner = isPartitioner;
	return this;
    }
}

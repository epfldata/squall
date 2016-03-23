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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;

import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.utilities.StormWrapper;
import ch.epfl.data.squall.utilities.ReaderProvider;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.operators.StoreOperator;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.NotAliveException;

import org.apache.log4j.Logger;

import backtype.storm.Config;

/* This class represents a context of execution. It provides a unified
 * interface for creating and submitting plans or running queries.
 */
public class SquallContext {
  private static Logger LOG = Logger.getLogger(SquallContext.class);

  private Config conf;
  private boolean local;
  private List<ReaderProvider> readerProviders;

  public SquallContext() {
    this(new Config());

    Map stormConf = backtype.storm.utils.Utils.readStormConfig();
    conf.putAll(stormConf);

    // Load default values
    SystemParameters.putInMap(conf, "DIP_EXTENSION", ".tbl");
    SystemParameters.putInMap(conf, "DIP_READ_SPLIT_DELIMITER", "\\|");
    SystemParameters.putInMap(conf, "DIP_GLOBAL_ADD_DELIMITER", "|");
    SystemParameters.putInMap(conf, "DIP_GLOBAL_SPLIT_DELIMITER", "\\|");

    SystemParameters.putInMap(conf, "DIP_KILL_AT_THE_END", "true");

    SystemParameters.putInMap(conf, "STORAGE_LOCAL_DIR", "/tmp/ramdisk");
    SystemParameters.putInMap(conf, "STORAGE_CLUSTER_DIR", "/data/squall_zone/storage");
    SystemParameters.putInMap(conf, "STORAGE_COLD_START", "true");
    SystemParameters.putInMap(conf, "STORAGE_MEMORY_SIZE_MB", "4096");

    SystemParameters.putInMap(conf, "DIP_NUM_ACKERS", 0);

    // TODO: load "local" from the configuration
  }

  public SquallContext(Config conf) {
    this.conf = conf;

    this.readerProviders = new ArrayList<ReaderProvider>(2);
    this.registerReaderProvider(new FileReaderProvider("."));
    this.registerReaderProvider(new FileReaderProvider("../test/data/tpch/0.01G/"));
    this.registerReaderProvider(new FileReaderProvider("./test/data/tpch/0.01G/"));
    // TODO: there should be a different provider for distributed mode
    this.registerReaderProvider(new FileReaderProvider("/data/tpch/0.01G/"));
  }

  @Deprecated
  public Config getConfiguration() {
    return conf;
  }

  public void submit(String name, QueryBuilder plan) {
    if (local) {
      submitLocal(name, plan);
    } else {
      submitDistributed(name, plan);
    }
  }

  public BasicStore<Object> submitLocalAndWait(String name, QueryBuilder plan) throws InterruptedException {
    setLocal();

    // TODO: name should be given in the plan somehow, as it is a property of
    // the query
    SystemParameters.putInMap(conf, "DIP_QUERY_NAME", name);
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", name);

    SystemParameters.putInMap(conf, "DIP_KILL_AT_THE_END", "true");

    // TODO: use parallelisms that were already set
    // TODO: take the parallelism from the component
    setAllParallelisms(plan);

    return StormWrapper.localSubmitAndWait(this, plan);
  }


  public Map<String,String> submitLocal(String name, QueryBuilder plan) {
    setLocal();

    // TODO: name should be given in the plan somehow, as it is a property of
    // the query
    SystemParameters.putInMap(conf, "DIP_QUERY_NAME", name);
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", name);

    SystemParameters.putInMap(conf, "DIP_KILL_AT_THE_END", "false");

    // TODO: use parallelisms that were already set
    // TODO: take the parallelism from the component
    setAllParallelisms(plan);
    StoreOperator storeOperator = new StoreOperator();
    plan.getLastComponent().getChainOperator().addOperator(storeOperator);


    StormWrapper.submitTopology(this, plan.createTopology(this));

    return storeOperator.getStore();
  }

  public void submitDistributed(String name, QueryBuilder plan) {
    setDistributed();

    // TODO: name should be given in the plan somehow, as it is a property of
    // the query

    SystemParameters.putInMap(conf, "DIP_QUERY_NAME", name);
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", name);

    SystemParameters.putInMap(conf, "DIP_KILL_AT_THE_END", "true");

    // TODO: use parallelisms that were already set
    // TODO: take the parallelism from the component
    setAllParallelisms(plan);

    StormWrapper.submitTopology(this, plan.createTopology(this));
  }

  private void setAllParallelisms(QueryBuilder plan) {
    for (String componentName: plan.getComponentNames()) {
      SystemParameters.putInMap(conf, componentName + "_PAR", "1");
    }
  }

  public void setLocal() {
    SystemParameters.putInMap(conf, "storm.cluster.mode", "local");
    SystemParameters.putInMap(conf, "DIP_DISTRIBUTED", "false");
    SystemParameters.putInMap(conf, "DIP_DATA_PATH", "../test/data/tpch/0.01G/");

    local = true;
  }

  public void setDistributed() {
    SystemParameters.putInMap(conf, "storm.cluster.mode", "distributed");
    SystemParameters.putInMap(conf, "DIP_DISTRIBUTED", "true");
    SystemParameters.putInMap(conf, "DIP_DATA_PATH", "/data/tpch/0.01G/");

    local = false;
  }

  public boolean isLocal() {
    return local;
  }

  public boolean isDistributed() {
    return !local;
  }

  public void registerReaderProvider(ReaderProvider provider) {
    readerProviders.add(0, provider);
  }

  public ReaderProvider getProviderFor(String resource) {
    ReaderProvider provider = null;

    Iterator<ReaderProvider> iterator = readerProviders.iterator();
    while (iterator.hasNext() && provider == null) {
      ReaderProvider next = iterator.next();
      if (next.canProvide(this, resource)) {
        provider = next;
      }
    }

    return provider;
  }

  public DataSourceComponent createDataSource(String table) throws IOException {
    String resource = table + SystemParameters.getString(conf, "DIP_EXTENSION");
    ReaderProvider provider = getProviderFor(resource);

    if (provider == null) {
      provider = getProviderFor(table);
      if (provider != null) {
        resource = table;
      }
    }

    if (provider == null) {
      String error = "Could not find table '" + table + "'. Registered providers in search order:\n";
      for(ReaderProvider p : readerProviders) {
        error = error + "\t" + p + "\n";
      }
      throw new IOException(error);
    }

    return new DataSourceComponent(table, provider, resource);
  }

  public ClusterSummary getStormClusterSummary() {
    return StormWrapper.getClusterInfo(isLocal(), conf);
  }

  public List<String> getQueries() {
    ClusterSummary cluster = getStormClusterSummary();

    List<String> result = new ArrayList();

    for (TopologySummary topology : cluster.get_topologies()) {
      result.add(topology.get_name());
    }

    return result;
  }

  public void killQuery(String name) {
    try {
      StormWrapper.killTopology(isLocal(), conf, name);
    } catch (NotAliveException e) {
      LOG.warn("Tried to kill topology '" + name + "' but it was not running.");
    }
  }

  private String getTopologyIdFromName(String name) {
    ClusterSummary cluster = getStormClusterSummary();
    String id = null;

    for (TopologySummary topology : cluster.get_topologies()) {
      if (topology.get_id().startsWith(name)) {
        id = topology.get_id();
      }
    }

    return id;
  }

  public String getQueryStatus(String name) {
    try {
      String id = getTopologyIdFromName(name);
      if (id != null) {
        TopologyInfo topology = StormWrapper.getTopology(isLocal(), conf, id);
        return topology.get_status();
      } else {
        return null;
      }
    } catch (NotAliveException e) {
      LOG.warn("Tried to get status for topology '" + name + "' but it was not running.");
      return null;
    }
  }

  public void setDbtoasterClassDir(String classdir) {
    SystemParameters.putInMap(conf, "squall.dbtoaster.classdir", classdir);
  }


}

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

import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.utilities.StormWrapper;

import org.apache.log4j.Logger;

import backtype.storm.Config;

/* This class represents a context of execution. It provides a unified
 * interface for creating and submitting plans or running queries.
 */
public class SquallContext {
  private static Logger LOG = Logger.getLogger(SquallContext.class);

  private Config conf;
  private boolean local;

  public SquallContext() {
    this(new Config());
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
  }

  // Useful snippet from https://xkcd.com/221/
  public int getRandomNumber() {
    return 4; // chosen by fair dice roll.
              // guaranteed to be random.
  }

  @Deprecated
  public Config getConfiguration() {
    return conf;
  }

  public void submit(String name, QueryBuilder plan) {
    if (local) {
      try {
        submitLocal(name, plan);
      } catch (InterruptedException e) {
        LOG.warn(e.getStackTrace());
      }
    } else {
      submitDistributed(name, plan);
    }
  }

  public BasicStore<Object> submitLocal(String name, QueryBuilder plan) throws InterruptedException {
    setLocal();

    // TODO: name should be given in the plan somehow, as it is a property of
    // the query
    SystemParameters.putInMap(conf, "DIP_QUERY_NAME", name);
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", name);

    // TODO: use parallelisms that were already set
    // TODO: take the parallelism from the component
    setAllParallelisms(plan);

    return StormWrapper.localSubmitAndWait(conf, plan);
  }

  public void submitDistributed(String name, QueryBuilder plan) {
    setDistributed();

    // TODO: name should be given in the plan somehow, as it is a property of
    // the query

    SystemParameters.putInMap(conf, "DIP_QUERY_NAME", name);
    SystemParameters.putInMap(conf, "DIP_TOPOLOGY_NAME", name);

    // TODO: use parallelisms that were already set
    // TODO: take the parallelism from the component
    setAllParallelisms(plan);

    StormWrapper.submitTopology(conf, plan.createTopology(conf));
  }

  private void setAllParallelisms(QueryBuilder plan) {
    for (String componentName: plan.getComponentNames()) {
      SystemParameters.putInMap(conf, componentName + "_PAR", "1");
    }
  }

  public void setLocal() {
    SystemParameters.putInMap(conf, "DIP_DISTRIBUTED", "false");
    SystemParameters.putInMap(conf, "DIP_DATA_PATH", "../test/data/tpch/0.01G/");

    local = true;
  }

  public void setDistributed() {
    SystemParameters.putInMap(conf, "DIP_DISTRIBUTED", "true");
    SystemParameters.putInMap(conf, "DIP_DATA_PATH", "/shared/tpch/0.01G/");

    local = false;
  }

  public boolean isLocal() {
    return local;
  }

  public boolean isDistributed() {
    return !local;
  }

}

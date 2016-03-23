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

package ch.epfl.data.squall.operators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class StoreOperator extends OneToOneOperator implements Operator {
    private static Logger LOG = Logger.getLogger(StoreOperator.class);

    private static final long serialVersionUID = 1L;

    private int _numTuplesProcessed = 0;
    private static ConcurrentMap<String, String> store = new ConcurrentHashMap<String, String>();

    public StoreOperator() {
    }

    public void accept(OperatorVisitor ov) {
      ov.visit(this);
    }

    @Override
    public List<String> getContent() {
      List<String> list = new ArrayList<String>();

      for(Map.Entry entry : this.store.entrySet()) {
        list.add(entry.getKey() + " = " + entry.getValue());
      }

      return list;
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    @Override
    public boolean isBlocking() {
	return true;
    }

    @Override
    public String printContent() {
      String result = "";
      for(String entry : getContent()) {
        result = result + entry + "\n";
      }
      return result;
    }

    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
      store.put(tuple.get(0), tuple.get(1));
      _numTuplesProcessed++;
      return tuple;
    }


    public Map<String,String> getStore() {
      return this.store;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("StoreOperator ");
	return sb.toString();
    }
}

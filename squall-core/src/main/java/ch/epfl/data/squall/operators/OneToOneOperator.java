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

import java.util.List;
import java.util.ArrayList;

public abstract class OneToOneOperator implements Operator {

  @Override
  public List<List<String>> process(List<String> tuple, long lineageTimestamp) {
    List<List<String>> resultList = new ArrayList<List<String>>();
    List<String> processed = processOne(tuple, lineageTimestamp);
    if (processed != null) {
      resultList.add(processed);
    }
    return resultList;
  }

  // Optional method
  protected abstract List<String> processOne(List<String> tuple, long lineageTimestamp);

}

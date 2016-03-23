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

import java.util.List;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.types.Type;

public interface JoinerComponent extends Component {
    public JoinerComponent setSlidingWindow(int windowRange);
    public long getSlidingWindow();

    public JoinerComponent setTumblingWindow(int windowRange);
    public long getTumblingWindow();

    public JoinerComponent setJoinPredicate(Predicate joinPredicate);
    public Predicate getJoinPredicate();


    // We have to offer the same interface as Component, but returning
    // JoinerComponent so we can stay in the more specific case instead of
    // upcasting to Component after each operation
    public JoinerComponent add(Operator operator);
    public JoinerComponent setBatchOutputMillis(long millis);
    public JoinerComponent setContentSensitiveThetaJoinWrapper(Type wrapper);
    public JoinerComponent setFullHashList(List<String> fullHashList);
    public JoinerComponent setHashExpressions(List<ValueExpression> hashExpressions);
    public JoinerComponent setOutputPartKey(int... hashIndexes);
    public JoinerComponent setOutputPartKey(List<Integer> hashIndexes);
    public JoinerComponent setPrintOut(boolean printOut);
}

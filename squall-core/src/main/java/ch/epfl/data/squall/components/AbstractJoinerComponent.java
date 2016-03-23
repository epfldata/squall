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

import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public abstract class AbstractJoinerComponent<C extends JoinerComponent> extends AbstractComponent<C> implements JoinerComponent {

    private long _windowSize = -1; // Width in terms of millis, Default is -1
				  // which is full history
    private long _latestTimeStamp = -1;

    private long _tumblingWindowSize = -1;// For tumbling semantics

    private Predicate _joinPredicate;

    public AbstractJoinerComponent(Component parent, String componentName) {
      super(parent,  componentName);
    }

    public AbstractJoinerComponent(Component[] parents, String componentName) {
      super(parents, componentName);
    }

    public AbstractJoinerComponent(List<Component> parents, String componentName) {
      super(parents, componentName);
    }

    public AbstractJoinerComponent(Component[] parents) {
      super(parents);
    }

    public AbstractJoinerComponent(List<Component> parents) {
      super(parents);
    }

    @Override
    public C setSlidingWindow(int windowRange) {
      WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
      // TODO: *1000?
      _windowSize = windowRange * 1000; // Width in terms of millis, Default
      // is -1 which is full history

      return getThis();
    }

    @Override
    public long getSlidingWindow() {
      return _windowSize;
    }

    @Override
    public C setTumblingWindow(int windowRange) {
        WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
        _tumblingWindowSize = windowRange * 1000;// For tumbling semantics
        return getThis();
    }

    @Override
    public long getTumblingWindow() {
      return _tumblingWindowSize;
    }

    @Override
    public Predicate getJoinPredicate() {
	return _joinPredicate;
    }

    @Override
    public C setJoinPredicate(Predicate joinPredicate) {
        _joinPredicate = joinPredicate;
        return getThis();
    }


}

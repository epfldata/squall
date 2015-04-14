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

package ch.epfl.data.squall.api.sql.optimizers.index;

import ch.epfl.data.squall.components.Component;

public class CompLevel implements Comparable<CompLevel> {
    private final Component _comp;
    private int _level;

    public CompLevel(Component comp, int level) {
	_comp = comp;
	_level = level;
    }

    @Override
    public int compareTo(CompLevel cl) {
	final int otherLevel = cl.getLevel();
	return (new Integer(_level)).compareTo(new Integer(otherLevel));
    }

    public Component getComponent() {
	return _comp;
    }

    public int getLevel() {
	return _level;
    }

    public void setLevel(int level) {
	_level = level;
    }
}

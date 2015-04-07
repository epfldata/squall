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


package ch.epfl.data.squall.thetajoin.dynamic.storm_component;

import java.util.List;

import ch.epfl.data.squall.storage.TupleStorage;
import ch.epfl.data.squall.storage.indexes.Index;

public class Quadruple {
	public TupleStorage affectedStorage, oppositeStorage;
	public List<Index> affectedIndexes, oppositeIndexes;

	public Quadruple(TupleStorage affectedStorage,
			TupleStorage oppositeStorage, List<Index> affectedIndexes,
			List<Index> oppositeIndexes) {
		this.affectedStorage = affectedStorage;
		this.oppositeStorage = oppositeStorage;
		this.affectedIndexes = affectedIndexes;
		this.oppositeIndexes = oppositeIndexes;
	}

}

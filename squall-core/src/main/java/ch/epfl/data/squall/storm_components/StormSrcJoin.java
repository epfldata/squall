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


package ch.epfl.data.squall.storm_components;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.ComponentProperties;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;

@Deprecated
public class StormSrcJoin implements StormEmitter, Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormSrcJoin.class);

	private StormSrcHarmonizer _harmonizer = null;
	private StormSrcStorage _firstStorage = null;
	private StormSrcStorage _secondStorage = null;

	private final String _componentName;
	private final List<Integer> _hashIndexes;
	private final List<ValueExpression> _hashExpressions;

	public StormSrcJoin(StormEmitter firstEmitter, StormEmitter secondEmitter,
			ComponentProperties cp, List<String> allCompNames,
			BasicStore<String> firstPreAggStorage,
			BasicStore<String> secondPreAggStorage,
			ProjectOperator firstPreAggProj, ProjectOperator secondPreAggProj,
			int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {

		_componentName = cp.getName();
		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();

		// set the harmonizer
		_harmonizer = new StormSrcHarmonizer(_componentName, firstEmitter,
				secondEmitter, builder, killer, conf);

		_firstStorage = new StormSrcStorage(firstEmitter, secondEmitter, cp,
				allCompNames, _harmonizer, true, firstPreAggStorage,
				firstPreAggProj, hierarchyPosition, builder, killer, conf);
		_secondStorage = new StormSrcStorage(firstEmitter, secondEmitter, cp,
				allCompNames, _harmonizer, false, secondPreAggStorage,
				secondPreAggProj, hierarchyPosition, builder, killer, conf);

		if (!MyUtilities.isAckEveryTuple(conf))
			throw new RuntimeException(
					"You must use StormDstJoin if you want to ACK only at the end!");
	}

	@Override
	public String[] getEmitterIDs() {
		return new String[] { _firstStorage.getID(), _secondStorage.getID() };
	}

	@Override
	public String getInfoID() {
		final StringBuilder sb = new StringBuilder();
		sb.append(_harmonizer.getInfoID()).append("\n");
		sb.append(_firstStorage.getInfoID()).append("\n");
		sb.append(_secondStorage.getInfoID()).append("\n");
		return sb.toString();
	}

	// from StormEmitter interface
	@Override
	public String getName() {
		return _componentName;
	}
}

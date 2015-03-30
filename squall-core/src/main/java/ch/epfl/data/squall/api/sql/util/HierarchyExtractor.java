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


package ch.epfl.data.squall.api.sql.util;

import java.util.List;
import java.util.Set;

import ch.epfl.data.squall.components.Component;

/*
 * A utility class for extracting different hierarchy-(topology-)related information
 */
public class HierarchyExtractor {

	private static boolean contains(Set<String> biggerSet,
			Set<String> smallerSet) {
		if (biggerSet.size() < smallerSet.size())
			return false;
		for (final String smallerElem : smallerSet)
			if (!biggerSet.contains(smallerElem))
				return false;
		return true;
	}

	public static Component getLCM(Component first, Component second) {
		Component resultComp = first;
		List<String> resultAnc = ParserUtil.getSourceNameList(resultComp);
		while (!resultAnc.contains(second.getName())) {
			resultComp = resultComp.getChild();
			resultAnc = ParserUtil.getSourceNameList(resultComp);
		}
		return resultComp;
	}

	public static Component getLCM(List<Component> compList) {
		Component resultLCM = getLCM(compList.get(0), compList.get(1));
		for (int i = 2; i < compList.size(); i++)
			resultLCM = getLCM(resultLCM, compList.get(i));
		return resultLCM;
	}

	/*
	 * Is component a child (not necessarily first generation) of all
	 * orCompNames? Used in Cost-based optimizer
	 */
	public static boolean isLCM(Component component, Set<String> orCompNames) {
		// dealing with parents
		final Component[] parents = component.getParents();
		if (parents == null)
			// if I don't have parents I can't be LCM (I am DataSourceComponent)
			return false;

		for (int i = 0; i < parents.length; i++) {
			final Component parent = parents[i];
			final Set<String> parentAncestors = ParserUtil
					.getSourceNameSet(parent);
			if (contains(parentAncestors, orCompNames))
				// my parent is LCM (or its parent)
				return false;
		}

		// if I contain all the mentioned sources, and none of my parent does
		// so, than I am LCM
		final Set<String> compAncestors = ParserUtil
				.getSourceNameSet(component);
		return contains(compAncestors, orCompNames);
	}

}
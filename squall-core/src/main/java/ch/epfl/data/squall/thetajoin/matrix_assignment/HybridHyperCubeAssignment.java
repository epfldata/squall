/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.thetajoin.matrix_assignment;

import java.util.Map;
import java.util.List;

public interface HybridHyperCubeAssignment {

	/**
	 * This method is used to get a list of reducers to which a given tuple must
	 * be sent.
	 * 
	 * @param columns
	 *            indicate which columns the tuple has.
	 * @return a list of index of reducers.
	 */
	public List<Integer> getRegionIDs(String emitterName, Map<String, Object> columns);
	
	/**
	 * Return the number of region divisions in a given dimension perspective
	 */
	public int getNumberOfRegions(String column);
	
	/**
	 * Return the partitioning result across each dimension
	 */
	public String getMappingDimensions();

}
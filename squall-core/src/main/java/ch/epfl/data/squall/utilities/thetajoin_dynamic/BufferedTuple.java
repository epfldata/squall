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


package ch.epfl.data.squall.utilities.thetajoin_dynamic;

public class BufferedTuple {

	private final String _componentName;
	private final String _tupleString;
	private final String _tupleHash;

	public BufferedTuple(String componentName, String tupleString,
			String tupleHash) {
		_componentName = componentName;
		_tupleString = tupleString;
		_tupleHash = tupleHash;
	}

	public String get_componentName() {
		return _componentName;
	}

	public String get_tupleHash() {
		return _tupleHash;
	}

	public String get_tupleString() {
		return _tupleString;
	}

}

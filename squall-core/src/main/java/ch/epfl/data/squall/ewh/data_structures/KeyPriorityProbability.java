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


package ch.epfl.data.squall.ewh.data_structures;

import java.io.Serializable;
import java.util.Comparator;

public class KeyPriorityProbability {
	public static class D2KeyProbabilityComparator implements
			Comparator<KeyPriorityProbability>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(KeyPriorityProbability kp1,
				KeyPriorityProbability kp2) {
			// obeys natural ordering
			if (kp1._d2KeyProbability < kp2._d2KeyProbability) {
				return -1;
			} else if (kp1._d2KeyProbability > kp2._d2KeyProbability) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	public static class KeyPriorityComparator implements
			Comparator<KeyPriorityProbability>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(KeyPriorityProbability kp1,
				KeyPriorityProbability kp2) {
			// obeys natural ordering: the smallest element is at the beginning
			// of the queue such that it can be quickly removed
			if (kp1._priority < kp2._priority) {
				return -1;
			} else if (kp1._priority > kp2._priority) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private String _key;

	private double _priority;

	private double _d2KeyProbability;

	public KeyPriorityProbability(String key, double priority,
			double d2KeyProbability) {
		_key = key;
		_priority = priority;
		_d2KeyProbability = d2KeyProbability;
	}

	public double getD2KeyProbability() {
		return _d2KeyProbability;
	}

	public String getKey() {
		return _key;
	}

	public double getPriority() {
		return _priority;
	}

	public void setD2KeyProbability(double d2KeyProbability) {
		_d2KeyProbability = d2KeyProbability;
	}

	@Override
	public String toString() {
		return "[Key, Priority, D2KeyProbability] = [" + _key + ", "
				+ _priority + ", " + _d2KeyProbability + "]";
	}
}
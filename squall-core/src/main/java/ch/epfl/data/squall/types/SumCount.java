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

package ch.epfl.data.squall.types;

import java.io.Serializable;

public class SumCount implements Comparable<SumCount>, Serializable {
    private static final long serialVersionUID = 1L;

    private Double _sum;
    private Long _count;

    public SumCount(Double sum, Long count) {
	_sum = sum;
	_count = count;
    }

    @Override
    public int compareTo(SumCount other) {
	if (getAvg() > other.getAvg())
	    return 1;
	else if (getAvg() < other.getAvg())
	    return -1;
	else
	    return 0;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (!(obj instanceof SumCount))
	    return false;
	final SumCount otherSumCount = (SumCount) obj;
	return getAvg() == otherSumCount.getAvg();
    }

    public double getAvg() {
	return _sum / _count;
    }

    public Long getCount() {
	return _count;
    }

    public Double getSum() {
	return _sum;
    }

    @Override
    public int hashCode() {
	int hash = 7;
	hash = 89 * hash + (_sum != null ? _sum.hashCode() : 0);
	hash = 89 * hash + (_count != null ? _count.hashCode() : 0);
	return hash;
    }

    public void setCount(Long count) {
	_count = count;
    }

    public void setSum(Double sum) {
	_sum = sum;
    }
}
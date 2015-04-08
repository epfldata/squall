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

//used for prefixSum in DenseMonotonicWeightPrecomputation and PWeightPrecomputation
public interface SimpleMatrix {

    // max num elements to store
    public long getCapacity();

    // num of elements stored (in the case of dense implementation (e.g.
    // int[][]), returns x * y
    public long getNumElements();

    public int getXSize();

    public int getYSize();

    public int getElement(int x, int y);

    public void setElement(int value, int x, int y);

    public void increment(int x, int y);

    public void increase(int delta, int x, int y);

}
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

package ch.epfl.data.squall.types;

public class MultiplicityType implements NumericType<Byte> {

    @Override
    public Byte fromDouble(double d) {
        return (byte) d;
    }

    @Override
    public Byte getMaxValue() {
        return Byte.MAX_VALUE;
    }

    @Override
    public Byte getMinPositiveValue() {
        return 0x01;
    }

    @Override
    public Byte getMinValue() {
        return Byte.MIN_VALUE;
    }

    @Override
    public Byte getOffset(Object base, double delta) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Byte minDecrement(Object obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Byte minIncrement(Object obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double toDouble(Object obj) {
        return (byte) obj;
    }

    @Override
    public Byte fromString(String str) {
        return Byte.valueOf(str);
    }

    @Override
    public double getDistance(Byte bigger, Byte smaller) {
        return bigger.doubleValue() - smaller.doubleValue();
    }

    @Override
    public Byte getInitialValue() {
        return 0x00;
    }

    @Override
    public String toString(Byte obj) {
        return obj.toString();
    }

    @Override
    public Byte generateRandomInstance() {
        throw new UnsupportedOperationException();
    }
}

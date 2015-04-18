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

public class DateLongType implements NumericType<Long> {

    private DateIntegerType _dateIntType = new DateIntegerType();

    @Override
    public Long fromDouble(double d) {
        return (long) _dateIntType.fromDouble(d);
    }

    @Override
    public Long fromString(String str) {
        return _dateIntType.fromString(str).longValue();
    }

    @Override
    public double getDistance(Long bigger, Long smaller) {
        return _dateIntType.getDistance(bigger.intValue(), smaller.intValue());
    }

    @Override
    public Long getOffset(Object base, double delta) {
        return _dateIntType.getOffset(base, delta).longValue();
    }

    @Override
    public Long getInitialValue() {
        return _dateIntType.getInitialValue().longValue();
    }

    @Override
    public Long minIncrement(Object obj) {
        return _dateIntType.minIncrement(obj).longValue();
    }

    @Override
    public Long minDecrement(Object obj) {
        return _dateIntType.minDecrement(obj).longValue();
    }

    @Override
    public Long getMinValue() {
        return _dateIntType.getMinValue().longValue();
    }

    @Override
    public Long getMinPositiveValue() {
        return _dateIntType.getMinPositiveValue().longValue();
    }

    @Override
    public Long getMaxValue() {
        return _dateIntType.getMaxValue().longValue();
    }

    @Override
    public double toDouble(Object obj) {
        return _dateIntType.toDouble(obj);
    }

    @Override
    public String toString(Long obj) {
        return _dateIntType.toString(obj.intValue());
    }

    public String toStringWithDashes(Long obj) {
        return _dateIntType.toStringWithDashes(obj.intValue());
    }

    @Override
    public Long generateRandomInstance() {
        return _dateIntType.generateRandomInstance().longValue();
    }
}

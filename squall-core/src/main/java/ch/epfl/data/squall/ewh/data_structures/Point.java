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

public class Point {
    private int _x, _y;

    public Point(int x, int y) {
	_x = x;
	_y = y;
    }

    public Point shift(int shiftX, int shiftY) {
	return new Point(_x + shiftX, _y + shiftY);
    }

    public void set_x(int x) {
	_x = x;
    }

    public void set_y(int y) {
	_y = y;
    }

    public int get_x() {
	return _x;
    }

    public int get_y() {
	return _y;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (_x ^ (_x >>> 32));
	result = prime * result + (_y ^ (_y >>> 32));
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	Point other = (Point) obj;
	if (_x != other._x)
	    return false;
	if (_y != other._y)
	    return false;
	return true;
    }
}

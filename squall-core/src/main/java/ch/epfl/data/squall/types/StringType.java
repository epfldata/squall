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

import java.util.Random;

public class StringType implements Type<String> {
    private static final long serialVersionUID = 1L;
    private final Random _rnd = new Random();

    @Override
    public String fromString(String str) {
	return str;
    }

    @Override
    public double getDistance(String bigger, String smaller) {
	throw new RuntimeException("Not applicable!");
    }

    @Override
    public String getInitialValue() {
	return "";
    }

    // for printing(debugging) purposes
    @Override
    public String toString() {
	return "STRING";
    }

    @Override
    public String toString(String obj) {
	return obj;
    }

    @Override
    public String generateRandomInstance() {
	return generateRandomString(30);
    }

    private String generateRandomString(int size) {
	char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
	StringBuilder sb = new StringBuilder();
	Random random = new Random();
	for (int i = 0; i < size; i++) {
	    char c = chars[random.nextInt(chars.length)];
	    sb.append(c);
	}
	return sb.toString();
    }

}
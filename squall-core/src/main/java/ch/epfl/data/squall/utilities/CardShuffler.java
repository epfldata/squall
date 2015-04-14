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

package ch.epfl.data.squall.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

//Used for ThetaJoin 1-Bucket tuple order (only for the paper)
//Idea from Hacking a Google Interview, Handout 3: 
//   http://www.google.ch/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&cad=rja&ved=0CCkQFjAA&url=http%3A%2F%2Fcourses.csail.mit.edu%2Fiap%2Finterview%2FHacking_a_Google_Interview_Handout_3.pdf&ei=c0jyUq_6Fsyp7AaFiIDYDg&usg=AFQjCNF3PlBJdjvO7nu-6t1HobPaDwoi4A&sig2=6E5_3WNS5yBCi-b_KcTIyQ&bvm=bv.60799247,d.bGQ

public class CardShuffler {

    private static void exchange(List<Integer> array, int first, int second) {
	int temp = array.get(first);
	array.set(first, array.get(second));
	array.set(second, temp);
    }

    public static void main(String[] args) {
	Random random = new Random();
	int position = 0;
	int offset = 16;
	int size = 16;

	List<Integer> arrayR = new ArrayList<Integer>(Arrays.asList(3, 3, 4, 4,
		4, 5, 7, 7, 9, 10, 11, 12, 12, 13, 13, 14));
	List<Integer> arrayS = new ArrayList<Integer>(Arrays.asList(3, 3, 4, 4,
		7, 7, 7, 8, 10, 11, 12, 13, 13, 14, 14, 15));

	for (int i = 0; i < size; i++) {
	    int exchangeIndex = position + random.nextInt(offset);
	    exchange(arrayR, i, exchangeIndex);
	    exchange(arrayS, i, exchangeIndex);
	    position++;
	    offset--;
	}

	System.out.println("Array R is " + arrayR);
	System.out.println("Array S is " + arrayS);
	StringBuilder sb = new StringBuilder("JoinMatrix: \n");
	for (int i = 0; i < size; i++) {
	    for (int j = 0; j < size; j++) {
		if (arrayR.get(i).equals(arrayS.get(j))) {
		    sb.append("1");
		} else {
		    sb.append("0");
		}
		if (j != size - 1) {
		    sb.append(", ");
		}
	    }
	    sb.append("\n");
	}
	System.out.println(sb.toString());
    }

}

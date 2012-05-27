/**
 * @author ElSeidy
 *  Utilities class used for mapping.
 */

/*
 * BUILDING = 3706
FURNITURE = 3007
MACHINERY = 2536
HOUSEHOLD = 2772
AUTOMOBILE = 2979
 */

package thetajoin.matrixMapping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utilities {
	
	/**
	 * Gets the prime factors for any integer.
	 * @param any number
	 * @return the prime factors
	 */
	
	public static List<Integer> primeFactors(int numbers) {
		int n = numbers; 
		List<Integer> factors = new ArrayList<Integer>();
		for (int i = 2; i <= n / i; i++) {
			while (n % i == 0) {
				factors.add(i);
				n /= i;
			}
		}
		if (n > 1) {
			factors.add(n);
		}
		return factors;
	}
	
	/**
	 * Generates the power set
	 * @param Set of numbers
	 * @return Set of sets
	 */
	public static <T> Set<List<T>> powerSet(List<T> originalSet) {
	    Set<List<T>> sets = new HashSet<List<T>>();
	    if (originalSet.isEmpty()) {
	        sets.add(new ArrayList<T>());
	        return sets;
	    }
	    List<T> list = new ArrayList<T>(originalSet);
	    T head = list.get(0);
	    List<T> rest = new ArrayList<T>(list.subList(1, list.size())); 
	    for (List<T> set : powerSet(rest)) {
	    	List<T> newSet = new ArrayList<T>();
	        newSet.add(head);
	        newSet.addAll(set);
	        sets.add(newSet);
	        sets.add(set);
	    }           
	    return sets;
	}
	
	/**
	 * Multiplies the contents of the list
	 * @param List of numbers
	 * @return The multiplication output of the 
	 */
	public static int multiply(List<Integer> list)
	{
		int mult=1;
		for (Integer value : list) {
			mult*=value;
		}
		return mult;
	}
	
}

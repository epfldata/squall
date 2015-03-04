package plan_runner.ewh.data_structures;

import java.io.Serializable;
import java.util.Comparator;

public class KeyPriorityProbability {
	private String _key;
	private double _priority;
	private double _d2KeyProbability;
	
	public KeyPriorityProbability(String key, double priority, double d2KeyProbability){
		_key = key;
		_priority = priority;
		_d2KeyProbability = d2KeyProbability;
	}
	
	public String getKey(){
		return _key;
	}
	
	public double getPriority(){
		return _priority;
	}
	
	public double getD2KeyProbability(){
		return _d2KeyProbability;
	}
	
	public void setD2KeyProbability(double d2KeyProbability) {
		_d2KeyProbability = d2KeyProbability;
	}	
	
	public String toString(){
		return "[Key, Priority, D2KeyProbability] = [" + _key + ", " +  _priority + ", " + _d2KeyProbability + "]";
	}
	
	public static class KeyPriorityComparator implements Comparator<KeyPriorityProbability>, Serializable{
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(KeyPriorityProbability kp1, KeyPriorityProbability kp2) {
			// obeys natural ordering: the smallest element is at the beginning of the queue such that it can be quickly removed
			if (kp1._priority < kp2._priority){
				return -1;
			}else if(kp1._priority > kp2._priority){
				return 1;
			}else{
				return 0;
			}
		}		
	}
	
	public static class D2KeyProbabilityComparator implements Comparator<KeyPriorityProbability>, Serializable{
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(KeyPriorityProbability kp1, KeyPriorityProbability kp2) {
			// obeys natural ordering
			if (kp1._d2KeyProbability < kp2._d2KeyProbability){
				return -1;
			}else if(kp1._d2KeyProbability > kp2._d2KeyProbability){
				return 1;
			}else{
				return 0;
			}
		}
	}
}
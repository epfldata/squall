package plan_runner.ewh.visualize;

import plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import plan_runner.ewh.data_structures.UJMPAdapterIntMatrix;

/*
 * Visitor interface
 */
public interface VisualizerInterface {
	
	public void visualize(UJMPAdapterByteMatrix m);
	public void visualize(UJMPAdapterIntMatrix m);
	
}
package ch.epfl.data.plan_runner.ewh.visualize;

import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterIntMatrix;

/*
 * Visitor interface
 */
public interface VisualizerInterface {

	public void visualize(UJMPAdapterByteMatrix m);

	public void visualize(UJMPAdapterIntMatrix m);

}
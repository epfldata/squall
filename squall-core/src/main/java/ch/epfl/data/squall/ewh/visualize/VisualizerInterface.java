package ch.epfl.data.squall.ewh.visualize;

import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterIntMatrix;

/*
 * Visitor interface
 */
public interface VisualizerInterface {

	public void visualize(UJMPAdapterByteMatrix m);

	public void visualize(UJMPAdapterIntMatrix m);

}
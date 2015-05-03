package ch.epfl.data.squall.storm_components;

// This is just a proxy for SynchronizedStormDataSource which is out of the squall-core 
public interface SynchronizedStormDataSourceInterface {
	
	public static String SHUFFLE_GROUPING_STREAMID = "sync_shuffle"; 
	
}

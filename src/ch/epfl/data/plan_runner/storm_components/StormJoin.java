package ch.epfl.data.plan_runner.storm_components;

public interface StormJoin extends StormEmitter {

	public static final int DST_ORDERING = 0;
	public static final int SRC_ORDERING = 1;

}

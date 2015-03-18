package ch.epfl.data.plan_runner.components;

public abstract class JoinerComponent implements Component{
	
	public long _windowSize = -1; // Width in terms of millis, Default is -1 which is full history
	public long _latestTimeStamp = -1;
	
	public long _tumblingWindowSize =-1;//For tumbling semantics
	
	public abstract Component setSlidingWindow(int windowRange);
	public abstract Component setTumblingWindow(int windowRange);

}

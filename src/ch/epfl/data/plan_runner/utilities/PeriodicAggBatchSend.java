package ch.epfl.data.plan_runner.utilities;

import java.util.Timer;
import java.util.TimerTask;

import ch.epfl.data.plan_runner.storm_components.StormComponent;

public class PeriodicAggBatchSend extends Timer {

	public class PeriodicTask extends TimerTask {
		private final StormComponent _comp;

		public PeriodicTask(StormComponent comp) {
			_comp = comp;
		}

		@Override
		public void run() {
			_comp.aggBatchSend();
		}
	}

	private final PeriodicTask _pt;

	private final StormComponent _comp;

	public PeriodicAggBatchSend(long period, StormComponent comp) {
		_comp = comp;
		_pt = new PeriodicTask(comp);

		scheduleAtFixedRate(_pt, 0, period);
	}

	public StormComponent getComponent() {
		return _comp;
	}

}

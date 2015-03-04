package plan_runner.ewh.data_structures;

public class FrequencyPosition {

	private int _frequency;
	private int _position;
	
	public FrequencyPosition(int frequency, int position){
		_frequency = frequency;
		_position = position;
	}

	public int getFrequency() {
		return _frequency;
	}

	public void setFrequency(int frequency) {
		_frequency = frequency;
	}

	public int getPosition() {
		return _position;
	}

	public void setPosition(int position) {
		_position = position;
	}
	
}

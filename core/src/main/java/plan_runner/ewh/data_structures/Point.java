package plan_runner.ewh.data_structures;

public class Point {
	private int _x, _y;

	public Point(int x, int y){
		_x = x;
		_y = y;
	}
	
	public Point shift(int shiftX, int shiftY){
		return new Point(_x + shiftX, _y + shiftY);
	}

	public void set_x(int x){
		_x = x;
	}

	public void set_y(int y){
		_y = y;
	}

	public int get_x(){
		return _x;
	}

	public int get_y(){
		return _y;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (_x ^ (_x >>> 32));
		result = prime * result + (int) (_y ^ (_y >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Point other = (Point) obj;
		if (_x != other._x)
			return false;
		if (_y != other._y)
			return false;
		return true;
	}
}

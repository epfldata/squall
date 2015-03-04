package plan_runner.ewh.data_structures;

public class MatrixIntInt implements SimpleMatrix {

	private int[][] _matrix;
	private int _xSize, _ySize;
	
	@Override
	public long getCapacity() {
		return ((long)_xSize) * _ySize;
	}

	@Override
	public long getNumElements() {
		return ((long)_xSize) * _ySize;
	}
	
	@Override
	public int getXSize() {
		return _xSize;
	}

	@Override
	public int getYSize() {
		return _ySize;
	}
	
	public MatrixIntInt(int xSize, int ySize){
		_xSize = xSize;
		_ySize = ySize;
		_matrix = new int[_xSize][_ySize];
	}
	
	@Override
	public int getElement(int x, int y) {
		return _matrix[x][y];
	}

	@Override
	public void setElement(int value, int x, int y) {
		_matrix[x][y] = value;
	}

	@Override
	public void increase(int delta, int x, int y) {
		_matrix[x][y] += delta;		
	}

	@Override
	public void increment(int x, int y) {
		increase(1, x, y);
	}
}
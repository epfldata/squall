package ch.epfl.data.plan_runner.ewh.visualize;

import java.util.List;

import org.ujmp.core.Matrix;

import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterIntMatrix;

public class UJMPVisualizer implements VisualizerInterface {
	// thickness of the rectangle line proportional to the overall dimension
	// size
	private int REGION_POINT_FACTOR = 800;

	private Matrix _visualMatrix;
	private int _xSize, _ySize;
	private String _label;

	public UJMPVisualizer(String label) {
		_label = label;
	}

	private void createRegions(List<Region> regions) {
		if (regions != null) {
			// adding regions to the matrix
			int numOfRegions = regions.size();
			for (int i = 0; i < numOfRegions; i++) {
				Region region = regions.get(i);
				drawRegion(i, numOfRegions, region);
			}
		}
	}

	private void drawLine(Matrix m, int x1, int y1, int x2, int y2, int xSize,
			int ySize, int colorRegion) {
		if (x1 == x2) {
			// horizontal line
			int aroundHor = xSize / REGION_POINT_FACTOR;
			int lowerBound = x1 - aroundHor;
			int upperBound = x1 + aroundHor;
			if (lowerBound == upperBound) {
				// avoids lines of zero width
				upperBound++;
			}
			for (int i = lowerBound; i < upperBound; i++) {
				for (int j = y1; j < y2; j++) {
					if (i >= 0 && i < xSize && j >= 0 && j < ySize) {
						m.setAsByte((byte) colorRegion, i, j);
					}
				}
			}
		} else if (y1 == y2) {
			// vertical line
			int aroundVer = ySize / REGION_POINT_FACTOR;
			int lowerBound = y1 - aroundVer;
			int upperBound = y2 + aroundVer;
			if (lowerBound == upperBound) {
				// avoids lines of zero width
				upperBound++;
			}
			for (int i = x1; i < x2; i++) {
				for (int j = lowerBound; j < upperBound; j++) {
					if (i >= 0 && i < xSize && j >= 0 && j < ySize) {
						m.setAsByte((byte) colorRegion, i, j);
					}
				}
			}
		} else {
			throw new RuntimeException("Weird line!");
		}
	}

	private void drawRegion(int regionNum, int numOfRegions, Region region) {
		int x1 = region.get_x1();
		int y1 = region.get_y1();
		int x2 = region.get_x2();
		int y2 = region.get_y2();
		int colorRegion = 255 * (regionNum + 1) / numOfRegions;
		drawLine(_visualMatrix, x1, y1, x1, y2, _xSize, _ySize, colorRegion);
		drawLine(_visualMatrix, x1, y1, x2, y1, _xSize, _ySize, colorRegion);
		drawLine(_visualMatrix, x2, y1, x2, y2, _xSize, _ySize, colorRegion);
		drawLine(_visualMatrix, x1, y2, x2, y2, _xSize, _ySize, colorRegion);
	}

	@Override
	public void visualize(UJMPAdapterByteMatrix m) {
		List<Region> regions = m.getRegions();
		if (regions == null) {
			_visualMatrix = m.getUJMPMatrix();
		} else {
			// regions change the matrix, that's why we have to clone
			_visualMatrix = m.getUJMPMatrix().clone();
			// none of the following two worked
			// Matrix matrixWithRegions = ujmpMatrix.select(Ret.LINK, "" + 0 +
			// "-" + (xSize - 1) + ";" + 0 + "-" + (ySize - 1));
			// Matrix matrixWithRegions = ujmpMatrix.select(Ret.LINK, new
			// int[]{0, xSize - 1}, new int[]{0, ySize -1});
		}

		_visualMatrix.setLabel(_label);
		_xSize = (int) _visualMatrix.getRowCount();
		_ySize = (int) _visualMatrix.getColumnCount();

		createRegions(regions);

		_visualMatrix.showGUI();
	}

	@Override
	public void visualize(UJMPAdapterIntMatrix m) {
		_visualMatrix = m.getUJMPMatrix();

		_visualMatrix.setLabel(_label);
		_xSize = (int) _visualMatrix.getRowCount();
		_ySize = (int) _visualMatrix.getColumnCount();

		_visualMatrix.showGUI();
	}
}
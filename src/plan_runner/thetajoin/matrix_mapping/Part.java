package plan_runner.thetajoin.matrix_mapping;

import java.io.Serializable;

/**
 * This class implement the notion of part: one element of a partition. A Part
 * is a rectangular area of the matrix.
 */

public class Part implements Serializable {

	private static final long serialVersionUID = 1L;

	// size of the part
	private final int height_;
	private final int width_;

	// Positioning in the general matrix
	private final int hIndex_;
	private final int wIndex_;

	// Constructor
	Part(int height, int width, int hIndex, int wIndex) {
		height_ = height;
		width_ = width;
		hIndex_ = hIndex;
		wIndex_ = wIndex;
	}

	// Method to see whether the partition is covering a given "pixel"
	public boolean covers(int pixelH, int pixelW) {
		return (hIndex_ <= pixelH) && (pixelH < hIndex_ + height_) && (wIndex_ <= pixelW)
				&& (pixelW < wIndex_ + width_);
	}

	public int getArea() {
		return height_ * width_;
	}

	// Geometric methods :
	public int getHalfPerimeter() {
		return (height_ + width_);
	}

	// Accessors :
	int getHeight() {
		return height_;
	}

	int getHIndex() {
		return hIndex_;
	}

	int getWidth() {
		return width_;
	}

	int getWIndex() {
		return wIndex_;
	}

	public boolean intersectColumn(int pixelH) {
		return (wIndex_ <= pixelH) && (pixelH < wIndex_ + width_);
	}

	public boolean intersectRow(int pixelW) {
		return (hIndex_ <= pixelW) && (pixelW < hIndex_ + height_);
	}
}

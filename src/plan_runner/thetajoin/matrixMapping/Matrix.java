package plan_runner.thetajoin.matrixMapping;

import java.io.Serializable;

/**
 * Class that encapsulate the set of parameters that characterize hyper parameters of a theta-join.
 * _height and _width represent the estimated sizes of both input relations of the theta-join.
 * The goal of defining this matrix is to determine the assignment of the theta-join to a set of workers,
 * based on a partitioning of the matrix. 
 * Once we get a partition of the matrix, each workers can be assign one part of it. 
 * A good partition would be one which balance the area assigned to each worker (load balancing),
 * and in which the area of each part is as close as possible to a square, (minimizing network utilisation).
 */

public class Matrix implements Serializable{
	
	private static final long serialVersionUID = 1L;
	/**
	 * Local representation:
	 * In the normal case, height_ correspond to the estimated size of S,
	 * width_ to the estimated size of T, and isInverted_ is false.
	 * If S is larger than T, then height_ correspond to the estimated size of T,
	 * width_ to the estimated size of S, and isInverted_ is true.
	 * The reason is that many algorithms assume |S| <= |T|.
	 */
	private int height_;
	private int width_;
	private boolean isInversed_;
	
	// Constructor
	public Matrix(int _sizeOfS, int _sizeOfT){
		
		if (_sizeOfT >= _sizeOfS){
			height_=_sizeOfS;
			width_=_sizeOfT;
			isInversed_=false;
		}else{
			height_=_sizeOfT;
			width_=_sizeOfS;
			isInversed_=true;
		}	
	}
	
	//Accessors:
	public int getHeight(){return height_;};
	public int getWidth(){return width_;};
	public int getSizeOfS(){return (isInversed_)?width_:height_;};
	public int getSizeOfT(){return (isInversed_)?height_:width_;};
	public boolean isSGreaterThanT(){return isInversed_;}
	
	public String toString(){
		String ret="";
		ret=ret.concat("Print of a matrix: \n");		
		ret=ret.concat("	size of S: " + getSizeOfS() + "\n");
		ret=ret.concat("	size of T: " + getSizeOfT() + "\n");
		return ret;
	}
}

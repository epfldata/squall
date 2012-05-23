package thetajoin.matrixMapping;

import java.io.Serializable;

/**
 * Class that encapsulate the set of parameters that characterize inputs of a theta-join.  
 */

public class Matrix implements Serializable{
	
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
	
	public void print(String _arg){
		System.out.println("Print of a matrix: " + _arg);		
		System.out.println("	size of S: " + getSizeOfS());
		System.out.println("	size of T: " + getSizeOfT());
	}
}

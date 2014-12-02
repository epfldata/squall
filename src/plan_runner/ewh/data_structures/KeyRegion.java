package plan_runner.ewh.data_structures;

import java.io.Serializable;
import java.util.List;

// JAT is Join Attribute Key
public class KeyRegion<JAT extends Comparable<JAT>> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	// region is defined with two opposite key points
	// upper keys are non-inclusive
	private JAT _kx1, _ky1, _kx2, _ky2;
	private int _regionIndex;
	// for the given key, what is the probability range that it ends up in this region (!Upper bound non-inclusive)
	private double _kx1ProbLowerPos, _kx1ProbUpperPos, _ky1ProbLowerPos, _ky1ProbUpperPos, _kx2ProbLowerPos, _kx2ProbUpperPos, _ky2ProbLowerPos, _ky2ProbUpperPos;
	
	public KeyRegion(JAT kx1, JAT ky1, JAT kx2, JAT ky2){
		_kx1 = kx1;
		_ky1 = ky1;
		_kx2 = kx2;
		_ky2 = ky2;
	}
	
	public KeyRegion(JAT kx1, JAT ky1, JAT kx2, JAT ky2, 
			double x1ProbLower, double x1ProbUpper, double y1ProbLower, double y1ProbUpper, 
			double x2ProbLower, double x2ProbUpper, double y2ProbLower, double y2ProbUpper, int regionIndex){
		this(kx1, ky1, kx2, ky2);
		_kx1ProbLowerPos = x1ProbLower;
		_kx1ProbUpperPos = x1ProbUpper;
		_ky1ProbLowerPos = y1ProbLower;
		_ky1ProbUpperPos = y1ProbUpper;
		_kx2ProbLowerPos = x2ProbLower;
		_kx2ProbUpperPos = x2ProbUpper;
		_ky2ProbLowerPos = y2ProbLower;
		_ky2ProbUpperPos = y2ProbUpper;
		_regionIndex=regionIndex;
	}
	
	public JAT get_kx1() {
		return _kx1;
	}

	public JAT get_ky1() {
		return _ky1;
	}

	public JAT get_kx2() {
		return _kx2;
	}

	public JAT get_ky2() {
		return _ky2;
	}
	
	public double get_kx1ProbLowerPos() {
		return _kx1ProbLowerPos;
	}

	public void set_kx1ProbLowerPos(double _kx1ProbLowerPos) {
		this._kx1ProbLowerPos = _kx1ProbLowerPos;
	}

	public double get_kx1ProbUpperPos() {
		return _kx1ProbUpperPos;
	}

	public void set_kx1ProbUpperPos(double _kx1ProbUpperPos) {
		this._kx1ProbUpperPos = _kx1ProbUpperPos;
	}

	public double get_ky1ProbLowerPos() {
		return _ky1ProbLowerPos;
	}

	public void set_ky1ProbLowerPos(double _ky1ProbLowerPos) {
		this._ky1ProbLowerPos = _ky1ProbLowerPos;
	}

	public double get_ky1ProbUpperPos() {
		return _ky1ProbUpperPos;
	}

	public void set_ky1ProbUpperPos(double _ky1ProbUpperPos) {
		this._ky1ProbUpperPos = _ky1ProbUpperPos;
	}

	public double get_kx2ProbLowerPos() {
		return _kx2ProbLowerPos;
	}

	public void set_kx2ProbLowerPos(double _kx2ProbLowerPos) {
		this._kx2ProbLowerPos = _kx2ProbLowerPos;
	}

	public double get_kx2ProbUpperPos() {
		return _kx2ProbUpperPos;
	}

	public void set_kx2ProbUpperPos(double _kx2ProbUpperPos) {
		this._kx2ProbUpperPos = _kx2ProbUpperPos;
	}

	public double get_ky2ProbLowerPos() {
		return _ky2ProbLowerPos;
	}

	public void set_ky2ProbLowerPos(double _ky2ProbLowerPos) {
		this._ky2ProbLowerPos = _ky2ProbLowerPos;
	}

	public double get_ky2ProbUpperPos() {
		return _ky2ProbUpperPos;
	}

	public void set_ky2ProbUpperPos(double _ky2ProbUpperPos) {
		this._ky2ProbUpperPos = _ky2ProbUpperPos;
	}
	public int getRegionIndex() {
		return _regionIndex;
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder("\nKeyRegion ");
		sb.append("with key boundaries kx1 = ").append(_kx1);
		sb.append(", ky1 = ").append(_ky1);
		sb.append(", kx2 = ").append(_kx2);
		sb.append(", ky2 = ").append(_ky2);
		
		sb.append("\n and their probabilities Prob(kx1) = [").append(_kx1ProbLowerPos).append(", ").append(_kx1ProbUpperPos).append(")");
		sb.append(", Prob(ky1) = [").append(_ky1ProbLowerPos).append(", ").append(_ky1ProbUpperPos).append(")");
		sb.append(", Prob(kx2) = [").append(_kx2ProbLowerPos).append(", ").append(_kx2ProbUpperPos).append(")");
		sb.append(", Prob(ky2) = [").append(_ky2ProbLowerPos).append(", ").append(_ky2ProbUpperPos).append(")");
		return sb.toString();
	}

	public static String toString(List<KeyRegion> keyRegions) {
		StringBuilder sb = new StringBuilder("\nKeyRegions are:");
		for(KeyRegion keyRegion: keyRegions){
			sb.append(keyRegion);
		}
		return sb.toString();
	}

}
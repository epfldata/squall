package ch.epfl.data.squall.thetajoin.dynamic.advisor;

import java.io.Serializable;

/**
 * Class to represent a value of type T or null.
 */
public class Maybe<T> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final T data;

	public Maybe() {
		this.data = null;
	}

	public Maybe(T data) {
		this.data = data;
	}

	public T get() {
		return data;
	}

	public boolean isNone() {
		return data == null;
	}
}

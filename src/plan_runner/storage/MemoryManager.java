package plan_runner.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

public class MemoryManager implements Serializable {

	Class thisClass;	
	Class partypes[];
	private Class mmclass;
	private long _maxSize;
	private long _currSize;
	transient private ObjectOutputStream _oos = null; 
	transient private ByteArrayOutputStream _baos = null;
	private static final long serialVersionUID = 1L;
	
	/* Size argument measured in MBytes */
	public MemoryManager(long maxSize) {
		// Setting up reflexion 
		this.partypes = new Class[2];
		this.thisClass = this.getClass();
		this.partypes[0] = new Object().getClass();
		this._currSize = 0;
		this._maxSize = (maxSize * 1024 * 1024);
//		this._maxSize = 2;
	}

	/* Primitive types */
	int getSize(int var) 	 { return 4; }
	int getSize(boolean var) { return 1; }
	int getSize(char var) 	 { return 2; }
	int getSize(byte var) 	 { return 1; }
	int getSize(short var) 	 { return 2; }
	int getSize(long var) 	 { return 8; }
	int getSize(float var) 	 { return 4; }
	int getSize(double var)  { return 8; }

	/* JAVA String and Primitive Wrappers */
	int getSize(String str) { 
		return str.getBytes().length;
	}

	private int ByteSize = 0;
	int getSize(Byte var) {
		if (ByteSize == 0) {
			ByteSize = genericGetSize(var);
		}
		return ByteSize;
	}
	
	private int ShortSize = 0;
	int getSize(Short var) {
		if (ShortSize == 0) {
			ShortSize = genericGetSize(var);
		}
		return ShortSize;
	}

	private int IntegerSize = 0;
	int getSize(Integer var) {
		if (IntegerSize == 0) {
			IntegerSize = genericGetSize(var);
		}
		return IntegerSize;
	}

	private int LongSize = 0;
	int getSize(Long var) {
		if (LongSize == 0) {
			LongSize = genericGetSize(var);
		}
		return LongSize;
	}

	private int FloatSize = 0;
	int getSize(Float var) {
		if (FloatSize == 0) {
			FloatSize = genericGetSize(var);
		}
		return FloatSize;
	}

	private int DoubleSize = 0;
	int getSize(Double var) {
		if (DoubleSize == 0) {
			DoubleSize = genericGetSize(var);
		}
		return DoubleSize;
	}

	private int CharacterSize = 0;
	int getSize(Character var) {
		if (CharacterSize == 0) {
			CharacterSize = genericGetSize(var);
		}
		return CharacterSize;
	}

	private int BooleanSize = 0;
	int getSize(Boolean var) {
		if (ByteSize == 0) {
			ByteSize = genericGetSize(var);
		}
		return ByteSize;
	}

	public int getSize(Object obj) {	
		/* It may happen -- for special tricks (see DistinctOperator)
		 * that we receive the null object. In this case return 0 since
		 * no additional memory size is required. */
		if (obj == null) 
			return 0;	
		try {
			partypes[1] = obj.getClass();	
			if (partypes[0].equals(partypes[1])) {
				// Avoid recursively calling this is you get a true Object as an argument!
				System.out.println("Generic get size called with object type " + partypes[1].getName());
				return genericGetSize(obj);
			}
			// Dynamic dispatch according to type
			Method m = thisClass.getDeclaredMethod("getSize", partypes[1]);
			return ((Integer)m.invoke(this, obj)).intValue();
		} catch (java.lang.NoSuchMethodException nsme) { 
			// We don't have a getSize for the specific type. No problem: serialize/deserialize
			System.out.println("Generic get size called with object type " + partypes[1].getName());
			return genericGetSize(obj);
		} catch (java.lang.IllegalAccessException iae) { 
			System.out.println("Squall MemoryManager:: IllegalAccessException encountered: " + iae.getMessage()); 
			System.exit(0);
		} catch (java.lang.reflect.InvocationTargetException ite) {
			System.out.println("Squall MemoryManager:: InvocationTargetException encountered: " + ite.getMessage()); 
			System.exit(0);
		}
		return 0;
	}

	private void initMemoryStreams() {
		this._baos = new ByteArrayOutputStream();
		try {
			this._oos = new ObjectOutputStream(this._baos);
		} catch (IOException ioe) { 
			System.out.println("Squall MemoryManager:: Couldn't initialize memory streams. IOException encountered: " + ioe.getMessage());
			System.exit(0);
		}
	}
	
	private int genericGetSize(Object obj) {
		if (this._baos == null || this._oos == null)
			initMemoryStreams();
		try {
			this._oos.reset();
			this._baos.reset();
			this._oos.writeObject(obj);
			this._oos.close(); 
			this._baos.close();
			return _baos.toByteArray().length;
		} catch (IOException ioe) {
			System.out.println("Squall MemoryManager:: genericGetSize() failed. Error while serializing/deserializing Object. IOException encountered:: " + ioe.getMessage()); 
			System.exit(-1);
		}
		return -1;
	}

	/* Checks if the store has enough bytes left to store bytesRequested
	 * size of objects */
	boolean hasExceededMaxSpace() {
		return (this._currSize > this._maxSize);
	}
	
	void allocateMemory(long bytes) {
		this._currSize += bytes;
	}

	void releaseMemory(Object obj) {
		long bytes = this.getSize(obj);
		//System.out.println("Releasing " + bytes + " bytes");
		this._currSize -= bytes;
		// Curr size can be less than zero, if store is evicting bigger elements
		// than the ones it registered. We handle this case here.
		if (this._currSize < 0)
			this._currSize = 0;
	}
}	

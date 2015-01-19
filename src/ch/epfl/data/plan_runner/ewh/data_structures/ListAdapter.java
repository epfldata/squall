package ch.epfl.data.plan_runner.ewh.data_structures;

public interface ListAdapter<T extends Comparable<T>> {
    public void add(T t);

    public void set(int index, T t);

    public T get(int index);

    public void remove(int index);

    public void sort();

    public int size();
}
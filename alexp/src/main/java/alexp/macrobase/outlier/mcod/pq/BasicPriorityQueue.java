package alexp.macrobase.outlier.mcod.pq;

public interface BasicPriorityQueue<E> {
    int size();

    E peek();
    E poll();

    boolean add(E item);
    boolean remove(E item);
    boolean contains(E item);

    void clear();
}

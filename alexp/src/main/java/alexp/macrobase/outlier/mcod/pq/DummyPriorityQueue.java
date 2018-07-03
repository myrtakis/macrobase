package alexp.macrobase.outlier.mcod.pq;

import org.apache.commons.lang3.NotImplementedException;

public class DummyPriorityQueue<E> implements BasicPriorityQueue<E> {
    @Override
    public int size() {
        return 0;
    }

    @Override
    public E peek() {
        throw new NotImplementedException(getClass().getSimpleName());
    }

    @Override
    public E poll() {
        throw new NotImplementedException(getClass().getSimpleName());
    }

    @Override
    public boolean add(E item) {
        return true;
    }

    @Override
    public boolean remove(E item) {
        return true;
    }

    @Override
    public boolean contains(E item) {
        return true;
    }

    @Override
    public void clear() {
    }
}

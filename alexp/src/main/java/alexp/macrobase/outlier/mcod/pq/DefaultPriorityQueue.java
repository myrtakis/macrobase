package alexp.macrobase.outlier.mcod.pq;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

public class DefaultPriorityQueue<E> extends PriorityQueue<E> implements BasicPriorityQueue<E> {
    public DefaultPriorityQueue() {
    }

    public DefaultPriorityQueue(Comparator<? super E> comparator) {
        super(comparator);
    }

    public DefaultPriorityQueue(Collection<? extends E> c) {
        super(c);
    }
}

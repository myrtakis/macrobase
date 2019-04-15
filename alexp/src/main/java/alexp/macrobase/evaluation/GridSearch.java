package alexp.macrobase.evaluation;

import com.google.common.collect.Streams;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class GridSearch {

    @FunctionalInterface
    public interface RunInstance {
        double accept(Map<String, Object> params) throws Exception;
    }

    private class SearchParam {
        final String name;
        final Object[] values;

        SearchParam(String name, Object[] values) {
            this.name = name;
            this.values = values;
        }
    }

    private ArrayList<SearchParam> searchParams = new ArrayList<>();

    private SortedMap<Double, Map<String, Object>> results = new TreeMap<>();

    private int threadsCount = Runtime.getRuntime().availableProcessors();

    private PrintStream out = System.out;

    public GridSearch addParam(String name, Object[] values) {
        searchParams.add(new SearchParam(name, values));
        return this;
    }

    public GridSearch addParams(Map<String, Object[]> params) {
        params.forEach(this::addParam);
        return this;
    }

    public void run(RunInstance runInstance) throws Exception {
        List<Object[]> permutations = new ArrayList<>();
        generatePermutations(searchParams.stream().map(p -> p.values).collect(Collectors.toList()), permutations, 0, new ArrayList<>());

        results.clear();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        Lock lock = new ReentrantLock();

        AtomicInteger num = new AtomicInteger();
        for (Object[] permutation : permutations) {
            executor.submit(() -> {
                // {"p1": x, "p2": y, ...}
                Map<String, Object> params = Streams.mapWithIndex(Arrays.stream(permutation), (o, i) -> new AbstractMap.SimpleEntry<>(searchParams.get((int) i).name, o))
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

                try {
                    double score = runInstance.accept(params);

                    lock.lock();
                    try {
                        out.println(String.format("%d/%d %s -> %.4f", num.incrementAndGet(), permutations.size(), params, score));

                        results.put(score, params);
                    } finally {
                        lock.unlock();
                    }
                } catch (Exception ex) {
                    Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), ex);
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    public SortedMap<Double, Map<String, Object>> getResults() {
        return results;
    }

    public void setThreadsCount(int threadsCount) {
        this.threadsCount = threadsCount;
    }

    public void setOutputStream(PrintStream out) {
        this.out = out;
    }

    private void generatePermutations(List<Object[]> paramsLists, List<Object[]> result, int depth, List<Object> current) {
        if(depth == paramsLists.size())
        {
            result.add(current.toArray());
            return;
        }

        for(int i = 0; i < paramsLists.get(depth).length; ++i)
        {
            List<Object> next = new ArrayList<>(current);
            next.add(paramsLists.get(depth)[i]);
            generatePermutations(paramsLists, result, depth + 1, next);
        }
    }
}

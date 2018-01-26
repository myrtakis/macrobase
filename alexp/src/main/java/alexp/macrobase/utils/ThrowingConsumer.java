package alexp.macrobase.utils;

/**
 * The same as Java8 Consumer<T> but allows to throw any exception
 */
@FunctionalInterface
public interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
}
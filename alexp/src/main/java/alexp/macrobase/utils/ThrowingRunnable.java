package alexp.macrobase.utils;

/**
 * The same as Java Runnable but allows to throw any exception
 */
@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Exception;
}
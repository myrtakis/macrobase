package alexp.macrobase.streaming.Windows;

import java.util.ArrayList;
import java.util.List;

/*
 *  Count based tumbling window implementation
 * */
public class Tumbling implements Window {

    int windowSize;
    boolean ready = false;
    List<String> window = new ArrayList<>();

    boolean endStream = false;

    public Tumbling(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void build(String dp) {

        if (!dp.isEmpty()) {
            window.add(dp);
            if (window.size() == windowSize) {
                ready = true;
            }
        } else {
            System.out.println("WINDOW SIZE ::::::::::::::::::::::: "+window.size());
            endStream = true;
            ready = true;
        }

    }

    @Override
    public List<String> getWindow() {
        return window;
    }

    @Override
    public boolean windowIsReady() {
        return ready;
    }

    @Override
    public boolean isEndStream() {
        return endStream;
    }

    @Override
    public void resetParams(int[] params) {
        this.windowSize = params[0];
    }

    @Override
    public void clearWindow() {
        window.clear();
        ready = false;
    }

}

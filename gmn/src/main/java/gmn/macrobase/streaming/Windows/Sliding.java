package gmn.macrobase.streaming.Windows;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/*
*  Count based sliding window implementation
* */
public class Sliding implements Window {

    int windowSize;
    int windowSlide;
    boolean ready = false;

    Queue<String> capacitor = new LinkedList<>();

    boolean endStream = false;

    public Sliding(int windowSize, int windowSlide) {
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }

    @Override
    public void build(String dp) {
        if (!dp.isEmpty()) {
            capacitor.add(dp);
            if (capacitor.size() >= windowSize + windowSlide) {
                ready = true;
            }
        } else {

            if (capacitor.size() <= windowSize) {
                windowSize = capacitor.size();
                endStream = true;
            }
            ready = true;
        }
    }

    @Override
    public List<String> getWindow() {
        List<String> temp = new ArrayList<>();
        List<String> queueAsList = new ArrayList<>(capacitor);
        for (int i = 0; i < windowSize; i++) {
            temp.add(queueAsList.get(i));
        }
        return temp;
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
        this.windowSlide = params[1];
    }


    @Override
    public void clearWindow() {

        for (int i = 0; i < windowSlide; i++) {
            capacitor.poll();
        }
        ready = false;

    }

}
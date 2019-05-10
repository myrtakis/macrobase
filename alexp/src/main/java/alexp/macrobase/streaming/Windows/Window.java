package alexp.macrobase.streaming.Windows;

import java.util.List;

public interface Window {

    void build(String dp);

    List<String> getWindow();

    boolean windowIsReady();

    boolean isEndStream();

    void resetParams(int[] params);

    void clearWindow();

}

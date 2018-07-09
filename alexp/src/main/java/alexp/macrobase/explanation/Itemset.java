package alexp.macrobase.explanation;

import java.util.Map;

public class Itemset {
    private Map<String, String> attributes;

    public Itemset(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}

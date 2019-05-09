package alexp.macrobase.explanation.hics.statistics.tests;

public enum TestNames {

    KOLMOGOROV_SMIRNOV_TEST("KolmogorovSmirnovTest"),
    WELTCH_TTEST("WeltchTTest");

    private final String text;

    /**
     * @param text
     */
    TestNames(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}

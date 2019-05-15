package alexp.macrobase.explanation.lookOut;

import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;

public class LookOut extends Explanation {

    private int budget = 6;     // How many plots (i.e. each plot consists of a dmax number of features) the algorithm will return. Default value is 6 plots
    private int dmax = 2;       // Indicates the dimensionality of the subspaces where the lookOut will examine to find the best dmax dimensional subspace. Defaul value is 2 dimensional subspaces


    /*
        CONSTRUCTORS
     */

    public LookOut(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) throws MacroBaseException {
        super(columns, classifierConf, explanationSettings);
    }

    @Override
    public <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces) {

    }

    /*
        OVERRIDE FUNCTIONS
     */

    @Override
    public void process(DataFrame input) throws Exception {

    }

    @Override
    public DataFrame getResults() {
        return null;
    }

    /*
        SETTER FUNCTIONS
     */

    public void setBudget(int budget) {
        this.budget = budget;
    }

    public void setDmax(int dmax) {
        this.dmax = dmax;
    }

}

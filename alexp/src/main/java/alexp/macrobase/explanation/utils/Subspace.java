package alexp.macrobase.explanation.utils;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

public class Subspace {
    private double score;

    private HashSet<Integer> features;

    public Subspace() {
        features = new HashSet<>();
    }

    public Subspace(HashSet<Integer> features, double score) {
        this.score = score;
        this.features = features;
    }

    public Subspace(double score) {
        this.score = score;
    }

    public Subspace(HashSet<Integer> features) {
        this.features = features;
    }

    /**
     * Copy Constructor
     * @param subspaceToCopy
     */
    public Subspace(Subspace subspaceToCopy) {
        this.features = new HashSet<>(subspaceToCopy.features);
        this.score = subspaceToCopy.score;
    }

    public HashSet<Integer> getFeatures() {
        return features;
    }

    public double getScore() {
        return score;
    }

    public int getDimensionality() {
        return features.size();
    }

    public void addFeature(int featureId) {
        if(features == null)
            features = new HashSet<>();
        features.add(featureId);
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "[" + score + ", " + features.toString() + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        return this.getFeatures().toString().equals(((Subspace) obj).getFeatures().toString());
    }

    @Override
    public int hashCode() {
        return features.toString().hashCode();
    }

    /**
     * Sort subspaces by their score in ascending order.
     */
    public static final Comparator<Subspace> SORT_BY_SCORE_ASC = (o1, o2) -> {
        if(o1.score == o2.score) {
            return 0;
        }
        return o1.score > o2.score ? 1 : -1;
    };

    /**
     * Sort subspaces by their score in descending order.
     */
    public static final Comparator<Subspace> SORT_BY_SCORE_DESC = (o1, o2) -> {
        if(o1.score == o2.score) {
            return 0;
        }
        return o1.score < o2.score ? 1 : -1;
    };


    public static final Comparator<Subspace> SORT_BY_FEATURES = (o1, o2) -> {
        Iterator<Integer> o1TreeSetIter = new TreeSet<>(o1.getFeatures()).iterator();
        Iterator<Integer> o2TreeSetIter = new TreeSet<>(o2.getFeatures()).iterator();
        while (o1TreeSetIter.hasNext() && o2TreeSetIter.hasNext()) {
            int o1FeatureId = o1TreeSetIter.next();
            int o2FeatureId = o2TreeSetIter.next();
            if (o1FeatureId < o2FeatureId)
                return -1;
            else if (o1FeatureId > o2FeatureId)
                return 1;
        }
        return 0;
    };

}

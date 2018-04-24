package alexp.macrobase.outlier.mcod.mtree;

import alexp.macrobase.outlier.mcod.Data;
import alexp.macrobase.outlier.mcod.utils.Pair;
import alexp.macrobase.outlier.mcod.utils.Utils;

import java.util.Set;

public class MTreeClass extends MTree<Data> {

    private static final PromotionFunction<Data> nonRandomPromotion = new PromotionFunction<Data>() {
        @Override
        public Pair<Data> process(Set<Data> dataSet, DistanceFunction<? super Data> distanceFunction) {
            return Utils.minMax(dataSet);
        }
    };

    public MTreeClass() {
        super(25, DistanceFunctions.EUCLIDEAN, new ComposedSplitFunction<Data>(nonRandomPromotion,
                new PartitionFunctions.BalancedPartition<Data>()));
    }

    public void add(Data data) {
        super.add(data);
        _check();
    }

    public boolean remove(Data data) {
        boolean result = super.remove(data);
        _check();
        return result;
    }

    public DistanceFunction<? super Data> getDistanceFunction() {
        return distanceFunction;
    }
}
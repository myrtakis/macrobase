import numpy as np
from sklearn.neighbors import LocalOutlierFactor
import pandas as pd
import itertools


def run_lof(dataframe, subspace):
    clf = LocalOutlierFactor(n_neighbors=15, algorithm='brute', metric='euclidean', contamination=0.015)
    clf.fit_predict(dataframe)
    x_scores = clf.negative_outlier_factor_
    points_rel_subspaces = {}
    sorted_ind = np.argsort(x_scores)
    for ind in sorted_ind:
        points_rel_subspaces[ind] = '[' + "".join(subspace) + '] ' + str(x_scores[ind]) + '; '
    return points_rel_subspaces


def test_subspace_2_3_4_5(dataframe, elementsToKeep):
    points_of_interest = [172, 183, 184, 207, 220, 245, 315, 323, 477, 510, 577, 654, 704, 723, 754, 765, 781, 824, 975]
    sub_dataframe = dataframe.iloc[:, [2, 3, 4, 5]]
    points_rel_subspaces = run_lof(sub_dataframe, sub_dataframe.columns)

    points_rel_subspaces = dict(itertools.islice(points_rel_subspaces.items(), elementsToKeep))
    print('\nPoints of interest\n')
    print(points_of_interest)

    print('\nPoints with top-' + str(elementsToKeep) + 'scores accompanied by their subspace and score in this '
                                                       'subspace\n')
    for pointId in points_rel_subspaces:
        print(str(pointId) + '=' + points_rel_subspaces[pointId])

    print('\nInliers scored higher than outliers\n')
    print(set(points_rel_subspaces.keys()) - set(points_of_interest))


if __name__ == '__main__':
    df = pd.read_csv('synth_multidim_010_000.csv')
    test_subspace_2_3_4_5(df, 5)
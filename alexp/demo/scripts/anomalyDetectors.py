import argparse
import time
import sys
from itertools import combinations
import re
import os
import pandas as pd
import numpy as np
from sklearn.neighbors import LocalOutlierFactor
from sklearn.ensemble import IsolationForest
from pyod.models import abod


#########################################################################
#                           ANOMALY DETECTORS                           #
#########################################################################


def lof(dataframe, params):
    if 'knn' not in params:
        params['knn'] = 15  # default value
    clf = LocalOutlierFactor(n_neighbors=int(params['knn']), algorithm='brute', metric='euclidean', contamination='auto')
    clf.fit_predict(dataframe)
    raw_scores = clf.negative_outlier_factor_
    return reverse_scores(normalize_scores(raw_scores))


def iforest(dataframe, params):
    if 'treesCount' not in params:
        params['treesCount'] = 100  # default value
    if 'subsampleSize' not in params:
        params['subsampleSize'] = 256  # default value
    clf = IsolationForest(n_estimators=int(params['treesCount']), max_samples=int(params['subsampleSize']), behaviour='new', contamination='auto')
    clf.fit(dataframe)
    raw_scores = clf.decision_function(dataframe)
    return reverse_scores(normalize_scores(raw_scores))


def fast_abod(dataframe, params):
    if 'n_neighbors' not in params:
        params['n_neighbors'] = 5  # default value
    clf = abod.ABOD(method='fast', n_neighbors=int(params['n_neighbors']))
    clf.fit(dataframe)
    raw_scores = clf.decision_scores_
    return normalize_scores(raw_scores)


FUNCTION_MAP = {
    'lof-bkaluza'   :   lof,
    'fast_abod'     :   fast_abod,
    'iforest'       :   iforest
}

#########################################################################
#                           UTIL FUNCTIONS                              #
#########################################################################


def normalize_scores(raw_scores):
    return np.interp(raw_scores, (raw_scores.min(), raw_scores.max()), (0, 1))


def reverse_scores(scores):
    return scores.max() - scores


def get_real_value_of(s):
    try:
        num =float(s)
        return num
    except ValueError:
        return s


def algorithm_params(params_str):
    if '=' not in params_str:
        return {}
    params_split = re.split('[,=]', params_str.strip('{}'))
    params_dict = {}
    for i in range(0, len(params_split), 2):
        key = params_split[i]
        value = params_split[i+1]
        params_dict[key] = get_real_value_of(value)
    return params_dict


def prepare_sub_dataframe(fp, subspace_str):
    fp = os.path.normpath(fp)
    dataframe = pd.read_csv(fp)
    subspace = subspace_to_list(subspace_str)
    dataframe = dataframe.iloc[:, subspace]
    return dataframe


def subspace_to_tuple(subspace_str):
    subspace = re.split(',', subspace_str.strip('[]'))
    return tuple(list(map(int, subspace)))


def poi_to_list(poi_str):
    if poi_str is None:
        return None
    else:
        poi = re.split(',', poi_str.strip('{}'))
        return list(map(int, poi))


def get_subspaces(args):
    if args.exhaust is not None:
        return list(combinations(range(int(args.dim)), int(args.exhaust)))
    elif args.sl is not None:
        return parse_subspace_list(args.sl)
    else:
        assert args.s is not None
        return [subspace_to_tuple(args.s)]


def parse_subspace_list(subspace_list_str):
    subspace_list = []
    parts = subspace_list_str.strip().split(';')
    for subspace_str in parts:
        if not subspace_str.strip():
            continue
        subspace_list.append(subspace_to_tuple(subspace_str))
    return subspace_list


def execute_option(parser):
    args = parser.parse_args()
    dataframe = pd.read_csv(os.path.normpath(args.d))
    params = algorithm_params(args.params)
    subspaces = get_subspaces(args)
    output = ''
    counter = 0
    for subspace in subspaces:
        subspace = subspace if isinstance(subspace, list) else list(subspace)
        counter += 1
        msg_prog = '> Scoring subspace ' + str(subspace) + ' (' + str(counter) + '/' + str(len(subspaces)) + ')'
        sys.stdout.write('\r' + msg_prog)
        sub_dataframe = dataframe.iloc[:, subspace]
        points_scores = FUNCTION_MAP[args.ad](sub_dataframe, params)
        output = output + '@subspace ' + str(subspace) + ' = ' + str(list(points_scores)) + '\n'
    print()
    return output


def options_builder():
    parser = argparse.ArgumentParser()
    parser.add_argument('-ad',          type=str,   required=True, help='Give the id of anomaly detector as it presents in configuration files')
    parser.add_argument('-params',      type=str,   required=True, help='Give the parameters of the algorithms')
    parser.add_argument('-s',           type=str,   help='A subspace type string in the form [0 1] where 0 and 1 are features')
    parser.add_argument('-sl',          type=str,   help='Take a list of subspaces to make the detection')
    parser.add_argument('-d',           type=str,   required=True, help='The path to the data frame')
    parser.add_argument('-dim',         type=str,   required=True, help='The dimensionality of the dataset')
    parser.add_argument('-exhaust',     type=int,   help="Makes exhaustive search i.e. scores every combination of attributes of a given dimensionality. Default value is 2")
    return parser


def run_exhaustive():
    return None


#########################################################################
#                               MAIN                                    #
#########################################################################


if __name__ == '__main__':

    opt_parser = options_builder()
    points_normalized_scores = execute_option(opt_parser)

    print(points_normalized_scores)
    #print(time.perf_counter())


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
#               LOADER CLASS FOR ARGUMENTS READ FROM FILE               #
#########################################################################


class LoadFromFile (argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        with values as f:
            args_list = f.read().split()
            for i in range(0, len(args_list)):
                if '%20' in args_list[i]:
                    args_list[i] = args_list[i].replace('%20', ' ')
                if '"' in args_list[i]:
                    args_list[i] = args_list[i].replace('"', '')
            parser.parse_args(args_list, namespace)


#########################################################################
#                           ANOMALY DETECTORS                           #
#########################################################################


def lof(dataframe, params, repeats=1):
    if 'knn' not in params:
        params['knn'] = 15  # default value
    scores = None
    for i in range(0, repeats):
        clf = LocalOutlierFactor(n_neighbors=int(params['knn']), algorithm='brute', metric='euclidean', contamination='auto')
        clf.fit_predict(dataframe)
        if scores is None:
            scores = clf.negative_outlier_factor_
        else:
            scores = scores + clf.negative_outlier_factor_
    scores = -scores
    return scores / repeats


def iforest(dataframe, params, repeats):
    if 'treesCount' not in params:
        params['treesCount'] = 100  # default value
    if 'subsampleSize' not in params:
        params['subsampleSize'] = 256  # default value
    scores = None
    for i in range(0, repeats):
        clf = IsolationForest(n_estimators=int(params['treesCount']), max_samples=int(params['subsampleSize']), behaviour='new', contamination='auto')
        clf.fit(dataframe)
        if scores is None:
            scores = clf.decision_function(dataframe)
        else:
            scores = scores + clf.decision_function(dataframe)
    scores = -scores
    scores = scores - scores.min()
    return scores / repeats


def fast_abod(dataframe, params, repeats=1):
    if 'n_neighbors' not in params:
        params['n_neighbors'] = 5  # default value
    scores = None
    for i in range(0, repeats):
        clf = abod.ABOD(method='fast', n_neighbors=int(params['n_neighbors']))
        clf.fit(dataframe)
        if scores is None:
            scores = clf.decision_scores_
        else:
            scores = scores + clf.decision_scores_
    scores = scores - scores.min()
    return scores / repeats


FUNCTION_MAP = {
    'lof-bkaluza'   :   lof,
    'fast_abod'     :   fast_abod,
    'iforest'       :   iforest
}

#########################################################################
#                           UTIL FUNCTIONS                              #
#########################################################################


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
    classifier_run_repeat = 1 if args.classifier_run_repeat <= 0 else args.classifier_run_repeat
    print()
    validate_args(parser, args)
    dataframe = pd.read_csv(os.path.normpath(args.d.replace('"', '')))
    params = algorithm_params(args.params)
    subspaces = get_subspaces(args)
    counter = 0
    for subspace in subspaces:
        subspace = subspace if isinstance(subspace, list) else list(subspace)
        counter += 1
        sub_dataframe = dataframe.iloc[:, subspace]
        msg_prog = '> Scoring subspace ' + str(counter) + '/' + str(len(subspaces)) + ': ' + \
                   str(subspace) + ' repeats: ' + str(classifier_run_repeat)
        print(msg_prog)
        points_scores = FUNCTION_MAP[args.ad](sub_dataframe, params, classifier_run_repeat)
        output = '\n' + '@subspace ' + str(subspace) + ' = ' + str(list(points_scores)) + '\n'
        print(output)


def validate_args(parser, args):
    assert args.ad is not None, parser.print_help()
    assert args.params is not None, parser.print_help()
    assert args.d is not None, parser.print_help()
    assert args.dim is not None, parser.print_help()
    assert (args.exhaust is not None or args.sl is not None or args.s is not None), parser.print_help()


def options_builder():
    parser = argparse.ArgumentParser()
    parser.add_argument('-ad',                      type=str,                           help='Give the id of anomaly detector as it presents in configuration files')
    parser.add_argument('-classifier_run_repeat',   type=int,   default=1,              help='How many times the anomaly detector will run')
    parser.add_argument('-params',                  type=str,                           help='Give the parameters of the algorithms')
    parser.add_argument('-s',                       type=str,                           help='A subspace type string in the form [0 1] where 0 and 1 are features')
    parser.add_argument('-sl',                      type=str,                           help='Take a list of subspaces to make the detection')
    parser.add_argument('-d',                       type=str,                           help='The path to the data frame')
    parser.add_argument('-dim',                     type=str,                           help='The dimensionality of the dataset')
    parser.add_argument('-exhaust',                 type=int,                           help="Makes exhaustive search i.e. scores every combination of attributes of a given dimensionality. Default value is 2")
    parser.add_argument('-args_from_file',          type=open,  action=LoadFromFile,    help='Read arguments from a text file')
    return parser


def run_exhaustive():
    return None


#########################################################################
#                                  MAIN                                 #
#########################################################################


if __name__ == '__main__':

    opt_parser = options_builder()
    execute_option(opt_parser)

    #print(time.perf_counter())

from collections.abc import Iterable
from decimal import Decimal as D
import itertools
import os


def remove_duplicated_lists(list_of_lists):
    # Given list of lists, remove lists that are already seen in the bigger container
    # e.g. [[A, B, C], [B, C, C], [A, B, C]] --> [[A, B, C], [B, C, C]]
    dedup_list = []
    for indiv_list in list_of_lists:
        if indiv_list not in dedup_list:
            dedup_list.append(indiv_list)
    return dedup_list


def remove_adjacent_repeats(list_of_lists):
    # Given list of lists with duplicated data in each list, remove tables that are the same that are right next to each other
    # e.g. [[A,A,B], [A,C,A]] --> [[A,B], [A,C,A]]
    dedup_list = []
    for indiv_list in list_of_lists:
        if len(indiv_list) > 0:
            previous_element = indiv_list[0]
            list_to_add = [previous_element]
            if len(indiv_list) > 1:
                for element in indiv_list:
                    if element != previous_element:
                        list_to_add.append(element)
                        previous_element = element
        else:
            list_to_add = []
        dedup_list.append(list_to_add)
    return dedup_list


def remove_duplicates(single_list):
    # remove any repeats in a single list
    dedup_list = []
    for x in single_list:
        if x not in dedup_list:
            dedup_list.append(x)
    return dedup_list


def pairwise(iterable):
    # s -> (s0,s1), (s1,s2), (s2, s3), ...
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def flatten(l):
    for el in l:
        if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


def find_file_types(directory_path, extension):
    return [x for x in os.listdir(directory_path) if x.endswith(extension)]


def reduce_precision(number, precision=2):
    try:
        pre_dec, post_dec = str(number).split('.')
    except ValueError:  # not a decimal
        return number

    if len(post_dec) > precision:
        number_str = pre_dec + '.' + post_dec[:precision]
        
        if type(number) in [D, float]:
            return D(number_str)
        return number_str
    return number

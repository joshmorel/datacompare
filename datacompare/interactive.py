# The interactive portion of the program

import pandas as pd
from datacompare.comparedataframe import CompareDataFrame

result = CompareDataFrame(pd.DataFrame({"Nums": [0,1, 2, 3], "Chars": ["zoo","a", "b", "c"]}, columns=['Nums', 'Chars']))
expected = CompareDataFrame(
    pd.DataFrame({"Nums": [1, 2, 3, 4, 5], "Chars": ["a", "b", "c", "d", "e"]}, columns=['Nums', 'Chars']))



def main():
    print_header()
    compare_loop()


def print_header():
    print('-------------------------')
    print('     DATA COMPARE APP    ')
    print('-------------------------')
    print()


# def get_dataset_from_user(dataset_number):
#     dataset_path = input('Provide directory to dataset number {}: '.format(dataset_number))
#     return(dataset_path)


def create_compare_frame(dataframe):
    return CompareDataFrame(dataframe)


def set_primary_key(comparedataframe, key):
    comparedataframe.set_primary_key(key)
    pass


# def show_member_in_left_not_in_right(left, right):
#     members_in_left_not_in_right = compare_membership(left, right)





def compare_loop():
    # simpledf = pd.DataFrame({"Nums": [3, 1, 0], "Chars": ["a", "b", "c"]}, columns = ['Nums','Chars'])
    # print('The simple dataframe index is {}'.format(simpledf.index))
    # print('The simple dataframe is {}'.format(simpledf))
    # compare_frame = create_compare_frame(simpledf)
    # print('The compare dataframe primary key is {}'.format(compare_frame.primary_key))
    # print('The compare dataframe index is {}'.format(compare_frame.index))
    # print('The compare dataframe is {}'.format(compare_frame))

    result = create_compare_frame(
        pd.DataFrame({"Nums": [1, 2, 3], "Chars": ["a", "b", "c"]}, columns=['Nums', 'Chars']))
    expected = create_compare_frame(
        pd.DataFrame({"Nums": [1, 2, 3, 4], "Chars": ["a", "b", "c", "d"]}, columns=['Nums', 'Chars']))



    while True:

        cmd = input('Do you [s]et primary key, check [m]embership, e[x]it? ')
        if cmd == 's':
            provided_key = input('Tell me the key: ')

            set_primary_key(comparedataframe=result, key=provided_key)
            set_primary_key(comparedataframe=expected, key=provided_key)

            print('The new key is {}'.format(result.primary_key))
        elif cmd == 'm':
            while True:
                print('Found member in result, not in expected with primary key {}.'.format(
                    next(compare_membership(result, expected))))
        else:
            print('Goodbye!')
            break
        print()

if __name__ == '__main__':
    main()

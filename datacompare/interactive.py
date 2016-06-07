# The interactive portion of the program

from datacompare.comparedataframe import CompareDataFrame
import pandas as pd


def main():
    print_header()
    # dateset_one = get_dataset_from_user(1)
    # dateset_two = get_dataset_from_user(2)


    compare_loop()
    # print('Dataset one is {}, and dataset two is located {}.'.format(dateset_one, dateset_two))


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


def compare_loop():
    simpledf = pd.DataFrame({"Nums": [3, 1, 0], "Chars": ["a", "b", "c"]}, columns = ['Nums','Chars'])
    print('The simple dataframe index is {}'.format(simpledf.index))
    print('The simple dataframe is {}'.format(simpledf))
    compare_frame = create_compare_frame(simpledf)
    print('The compare dataframe primary key is {}'.format(compare_frame.primary_key))
    print('The compare dataframe index is {}'.format(compare_frame.index))
    print('The compare dataframe is {}'.format(compare_frame))

    while True:

        cmd = input('Do you [s]et primary key, e[x]it? ')
        if cmd == 's':
            provided_key = input('Tell me the key: ')

            set_primary_key(comparedataframe=compare_frame, key=provided_key)
            print('The new key is {}'.format(compare_frame.primary_key))
        else:
            print('Goodbye!')
            break
        print()

if __name__ == '__main__':
    main()

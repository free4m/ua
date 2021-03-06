from glob import glob
import pandas as pd
import datetime


def date_search(data, start_date, end_date):
    """
    Given the full data set, return a smaller data set that represents data within the date parameters specified.

    :param data:
    :param start_date:
    :param end_date:
    """
    # change dates for date search
    data['timestamp'] = pd.to_datetime(data['timestamp']).dt.date
    d1 = datetime.datetime.strptime(f'{start_date}', '%Y-%m-%d').date()
    d2 = datetime.datetime.strptime(f'{end_date}', '%Y-%m-%d').date()

    # constrict data by date search parameters
    less_data = data[(data['timestamp'] >= d1) & (data['timestamp'] <= d2)]

    return less_data


def create_count(less_data):
    """
    Given a data set, create and return a new data set containing a computed column that represents the count of
    each food_id.

    :param less_data:
    """
    # create count and distinct list
    less_data['count'] = less_data.groupby('food_id')['food_id'].transform('count')
    less_data = less_data[['food_id', 'count']].drop_duplicates().sort_values(by=['count'], ascending=False)

    return less_data


def create_weight(less_data):
    """
    Given a data set, create a return a new data set containing a computed column that represents a weight value for use
    in sorting search results.

    :param less_data:
    """
    # create data used in weight calc
    less_data['count'] = less_data.groupby('food_id')['food_id'].transform('count')
    less_data['diff'] = (datetime.datetime.now().date() - less_data['timestamp']).astype('timedelta64[D]')
    less_data['avg_diff'] = less_data.groupby('food_id')['diff'].transform('mean')
    less_data['rank'] = less_data['avg_diff'].rank(method='dense', ascending=False)

    # calculate weight
    less_data['weight'] = ((less_data['count'] * less_data['rank']) / len(less_data)) * 1000
    less_data = less_data[['food_id', 'weight']].drop_duplicates().sort_values(by=['weight'], ascending=False)

    return less_data


if '__main__':

    # create dataframe from .csv log files
    print("Creating the full data set...")
    files = [i for i in glob('./food_entries/*.csv')]
    data = pd.concat([pd.read_csv(f) for f in files])

    # constrict data
    print("Creating the search results...")
    data = date_search(data, '2017-03-01', '2017-09-01')

    # create counted object
    print("Creating the counted results...")
    count_thing = create_count(data)
    count_thing.to_csv('food_counted.csv', index=False, header=True)

    # create weighted object
    print("Creating the weighted results...")
    weight_thing = create_weight(data)
    weight_thing.to_csv('food_weighted.csv', index=False, header=True)

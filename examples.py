import time
import datetime

from settings import disciplines
from springer_search import SpringerSearch

if __name__ == '__main__':
    """EXAMPLES:
    Comment/uncomment what you want to try
    """
    start = time.time()
    spr = SpringerSearch()

    """1) Create query with different constraints (query can be used in other methods later)"""
    # q = spr.create_query(year=2018, title="rocket")
    # print(q)

    """2) Collect summary data by query"""
    # spr.get_info_by('subject:"Medicine %26 Public Health"')
    # print(spr.data)

    """3) Save summary info about records(articles etc.) to file (20 records limit to be found)"""
    # spr.get_all_records('subject:"Medicine & Public Health"', 20)

    """4) Collect all possible records by all disciplines
     in the specified time interval with saving to files"""
    for disc in disciplines:
        qry = spr.create_query(subject=disc,
                               onlinedatefrom='2000-01-01',
                               onlinedateto='2022-05-01')
        spr.get_all_records(qry, total=500)

    """5) Create dataframe with count of publications by countries from 2019 year for 'Engineering' discipline"""
    # df = spr.collect_statistic_by_years('Engineering', category='country', from_=2019)
    # print(df)

    """6) Create DataFrame with numbers of publications by category with specified query"""
    # df = spr.create_dataframe_by_category(catg='subject', onlinedatefrom='2022-01-01', onlinedateto='2022-02-01')
    #  same to:
    # qr = spr.create_query(onlinedatefrom='2022-01-01',
    #                       onlinedateto='2022-02-01')
    # df_2 = spr.create_dataframe_by_category('subject', qr)

    print(str(datetime.timedelta(seconds=round(time.time() - start))))

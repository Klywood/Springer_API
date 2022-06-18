import csv
import os
import datetime
import json
from typing import Union, Optional, List, Any

import requests
import pandas as pd

from api_token import TOKEN
from settings import disciplines, categories, folder
from logger import Logger

#  current date
now = datetime.datetime.now()


class SpringerSearch(Logger):
    """Class to search info from https://link.springer.com/"""

    def __init__(self, prefix: str = 'Springerlink'):
        """Initialize method"""
        #  create logger
        super().__init__(prefix)
        #  query
        self.__query = None
        #  total number of records for response
        self.__total_records = 0
        #  number of shown records in 1 request (100 - max)
        self.__step = 50
        #  collected data
        self.__data = None
        #  create folder to store data
        os.makedirs(folder, exist_ok=True)

    @property
    def query(self) -> str:
        """Getter for query attribute"""
        return self.__query

    @query.setter
    def query(self, query: str):
        """Setter for query attribute"""
        if not isinstance(query, str):
            raise ValueError("Query must be a string!")
        #  replace '&' symbol with URL-code
        self.__query = query.replace('&', '%26')

    @property
    def data(self):
        """Getter for data attribute"""
        return self.__data

    @staticmethod
    def __validate_year(year: int):
        """Validation method for year constraint"""
        if int(year) < 1832:
            raise ValueError("There is no information before 1832")
        if int(year) > now.year:
            raise ValueError(f"There is no information for {year}. Current year is {now.year}")

    @staticmethod
    def __validate_data(value: str, valid_values: Union[tuple, list, dict], data_type: str):
        """Validation method for some data

        Parameters:
            value: the value to be validated
            valid_values: valid data (examples in settings file)
            data_type: value type name (for logs)
        """
        if value not in valid_values:
            raise ValueError(f'Incorrect {data_type} "{value}". '
                             f'Visit main page: https://link.springer.com/ '
                             f'and API documentation: https://dev.springernature.com/')

    def create_query(self, **constraints) -> str:
        """Method for creating query-string by constraints


        | Watch possible subjects at https://link.springer.com/
        | constraint params at https://dev.springernature.com/adding-constraints

        | Some constraints:
        | 'subject' - specified scientific discipline (Mathematics, Engineering, etc.);
        | 'keyword' - limit to articles tagged with a keyword;
        | 'title' - locate documents containing a word or phrase in the "article/chapter title" element;
        | 'year' - articles/chapters published in a particular year;
        | 'onlinedatefrom (onlinedateto)' - limit to date range that an article appeared online
            in 'y-m-d' format ('2019-05-27');

        Parameters:
            constraints: constraints with values

        Examples:

            With two constraints:
                create_query(subject='Physics', year='2019')

            Will raise the 'Incorrect subject' exception:
                create_query(subject='Math')

        Raises:
            TypeError: if no constraints specified
            ValueError: if the year is less than 2003
            ValueError: if subject is incorrect

        Return:
            query-string
        """
        if constraints:
            #  year validation
            if 'year' in constraints:
                self.__validate_year(constraints['year'])
            #  discipline validation
            if 'subject' in constraints:
                self.__validate_data(constraints['subject'], disciplines, 'discipline')
                constraints['subject'] = constraints['subject'].replace('&', '%26')
            query = ''
            for key, value in constraints.items():
                query += f'{key}:"{value}" '
        else:
            raise TypeError("Specify at least one constraint!")
        #  exclude last space in query
        self.add_log(f"Created '{query}' query")
        return query[:-1]

    def get_info_by(self, query: str, start_from: int = 1, res_count: int = 0):
        """Requests to Springer API

        Parameters:
            query: query-string or string with complex constraints
                (Use create_query method to create query or watch examples for special constraints)
            start_from: return results starting at the number specified
            res_count: number of results to return in this request

        Examples:
            Using Multiple Constraints:
                constraints = 'title:"game theory" OR title:"perfect information"'
            Using Multi-Word Phrase:
                constraints = 'orgname:"University of Calgary"'
            Excluding Constraints:
                constraints = 'journal:"Planta" name:"Smith" -(name:"Fry")'
        """
        try:
            #   query string
            self.query = query
            url = f'https://api.springernature.com/metadata/json?' \
                  f'q={self.query}&s={start_from}&p={res_count}&api_key={TOKEN}'
            print(url)
            self.add_log(f"Making a '{self.query}' request")
            #  firs request with only main info
            response = requests.get(url)
            print(response.status_code)
            self.__data = json.loads(response.text)
            self.add_log("Response was saved in the 'date' attribute")
        except Exception as exc:
            self.add_log(f"Error occurred: {exc}", 'WARNING')

    def get_all_records(self, query: str, total: int = None):
        """Collect info from all records

        Parameters:
            query: query-string (examples in get_info_by method)
            total: limit on total number of records to be found
                (None as default - collecting all possible records)
        """
        #  request to API
        self.get_info_by(query)
        #  getting total records number
        self.__get_records_number(total)
        #  name of file to save data
        file_to_save = '_'.join(self.query.replace('"', '').replace(':', '-').split())
        full_path = os.path.join(folder, file_to_save)
        #  counters
        total_saved = 0
        current_record = 1
        #  counter of iterations without saving new records
        looped = 0
        while total_saved < total:
            count = 0
            self.add_log(f"Getting {current_record}-{current_record + self.__step} records")
            #  get records from API
            self.get_info_by(query, current_record, self.__step)
            #  go through records
            for record in self.__data['records']:
                #  take valid records by 'keyword' key
                if any(k in record.keys() for k in ['keywords', 'keyword', 'abstract']):
                    #  create dict with main information from record
                    rec_info = self.__parse_records(record)
                    #  save
                    self.__save_record(rec_info, full_path)
                    count += 1
            self.add_log(f"Saved {count} records at current iteration")
            total_saved += count
            looped += 1 if count == 0 else 0
            #  after 5 iterations without new records - break the loop
            if looped > 5:
                self.add_log("There are no new records. Break the loop", 'INFO')
                break
            self.add_log(f"Total saved: {total_saved} of {self.__total_records} records", "INFO")
            current_record += self.__step

    def __get_records_number(self, total: int = None):
        """Gets the total number of records by request and set iteration step

        Parameters:
            total: limit on total number of records to be found (sets in _get_all_records method)
        """
        #  getting the total records number
        if total:
            self.__total_records = total
            self.__step = min(self.__step, total)
        else:
            self.__total_records = int(self.data["result"][0]["total"])
        self.add_log(f"There are {self.__total_records} records to parse", 'INFO')

    def __get_main_info(self):
        """Return summary information for request"""
        return self.data["facets"]

    def collect_statistic_by_years(self, discipline: str, category: str = 'subject',
                                   from_: Union[int, str] = 2003, to_: Union[int, str] = now.year,
                                   set_index: bool = False) -> pd.DataFrame:
        """Collect statistic by specified discipline and category in the specified range of years

        Parameters:
            discipline: the scientific field. Possible disciplines:
                    'Biomedicine', 'Business and Management', 'Chemistry', 'Computer Science', 'Earth Sciences',
                    'Economics', 'Education', 'Engineering', 'Environment', 'Geography', 'History', 'Law',
                    'Life Sciences', 'Literature', 'Materials Science', 'Mathematics', 'Medicine & Public Health',
                    'Pharmacy', 'Philosophy', 'Physics', 'Political Science and International Relations',
                    'Psychology', 'Social Sciences', 'Statistics'
            category: the category for which statistics will be collected ('subject' as default).
                Possible categories: 'subject', 'keyword', 'pub', 'year', 'country', 'type'
            from_: the date(year) from which statistics will be collected (no info before 2003 - default value)
            to_: last date(year) (included) - current year as default
            set_index: set 'category' column as index of data frame (False as default)

        Examples:
            Using only discipline: get counts of publication by sections for 'Medicine & Public Health':
                 df = collect_statistic_by_years('Medicine & Public Health')

            Using several params: get counts of publication by countries for 'Computer Science'
            from 2019 to current year:
                    df = collect_statistic_by_years('Computer Science', category='country', from_year=2019)

        Return:
            data frame with collected statistic
        """
        #  validate discipline
        self.__validate_data(discipline, disciplines, 'discipline')
        #  validate category
        self.__validate_data(category, categories, 'category')
        #  validate years values
        self.__validate_year(from_)
        self.__validate_year(to_)
        #  create dataframe with target column
        dt_frame = pd.DataFrame(columns=[category])
        #  go through years
        for yr in range(from_, to_ + 1):
            #  create another df by year
            tmp_df = self.create_dataframe_by_category(category,
                                                       col_name=f'count_{yr}',
                                                       subject=discipline, year=yr)
            #  merge to main
            dt_frame = dt_frame.merge(tmp_df, how='right')
            self.add_log(f"Info for {yr} year added to data frame")
        #  processing dataframe
        dt_frame.fillna(0, inplace=True)
        dt_frame.iloc[:, 1:] = dt_frame.iloc[:, 1:].astype('int')
        if set_index:
            dt_frame.set_index(category, inplace=True)

        title = f"{discipline}_by_'{category}'_from_'{from_}'_to_'{to_}'"
        self.add_log(f"Done! DataFrame for '{title}' created", 'INFO')
        return dt_frame

    def create_dataframe_by_category(self, catg: str, query: str = None,
                                     col_name: str = 'count', **kwargs) -> pd.DataFrame:
        """
        Create dataframe with numbers of publications by category with specified query

        Parameters:
            catg: category to collect statistics by
            query: query-string for SpringerAPI
            col_name: name of the column with stored data
            kwargs: named arguments for creating query

        Return:
            dataframe with numbers of publications by specified category
        """
        query = query if query else self.create_query(**kwargs)
        self.get_info_by(query)
        data = self.__get_main_info()
        dataframe = pd.read_json(json.dumps(data[categories[catg]]['values'])). \
            rename(columns={"value": catg, "count": col_name})
        return dataframe

    def __parse_records(self, record: dict) -> tuple:
        """Get the main info about record

        Parameters:
            record: dictionary with all info about record

        Return:
            tuple with summary information
        """
        try:
            summary = (record.get("contentType"),
                       record.get("language"),
                       record.get('url')[0]['value'] if record.get('url') else None,
                       record.get('title'),
                       self.__get_creators(record),
                       record.get('publicationName'),
                       record.get('publicationDate'),
                       record.get('abstract'),
                       self.__get_keywords(record)
                       )
            return summary
        except Exception as err:
            self.add_log(f"Error during processing the record: {err}")

    @staticmethod
    def __get_keywords(rec: dict) -> Optional[List[Any]]:
        """Get keywords from record in correct format

        Parameters:
            rec: dictionary with all info about record

        Return:
            list with keywords
        """
        if any(i not in rec.keys() for i in ['keyword', 'keywords']):
            return None
        #  key name
        key = 'keywords' if 'keywords' in rec.keys() else 'keyword'
        return list(filter(lambda x: '  ' not in x, rec[key]))

    @staticmethod
    def __get_creators(rec) -> Union[list, None]:
        """Get creators from record

        Parameters:
            rec: dictionary with all info about record

        Return:
            list with creators
        """
        creators = rec.get('creators')
        if isinstance(creators, dict):
            return [i['creator'] for i in creators]
        elif isinstance(creators, list):
            return creators
        return None

    @staticmethod
    def __save_record(record: tuple, filename: str):
        """Save info to csv-file

        Parameters:
            record: dictionary with summary info to be saved
            filename: name of '.csv' file to save data
        """
        filename = f'{filename}.csv'
        with open(filename, 'a', encoding='utf-8') as file:
            writer = csv.writer(file)
            #  if file is empty - write labels
            if os.stat(filename).st_size == 0:
                writer.writerow(
                    ['type', 'language', 'url', 'title', 'creators',
                     'source/applicant', 'publication_date', 'abstract', 'keywords'])
            writer.writerow(record)


if __name__ == '__main__':
    spr = SpringerSearch()
    for yr in range(2000, 2023):
        qry = spr.create_query(subject='Computer Science',
                               year=yr
                               )
        spr.get_all_records(qry, total=5000)

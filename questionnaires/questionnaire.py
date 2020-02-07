"""
Created on: January 17, 2020.
Last modified on: February 7, 2020.
Copyright by Dzmitry A. Kaliukhovich.
E-mail: <first name>.<last name> AT gmail.com

The code in this file is distributed under the conditions of MIT License.

Copyright (c) 2020, Dzmitry A. Kaliukhovich.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


from datetime import datetime as dt
from dateutil import tz
import logging
import mysql.connector
import os
import pandas
import pysftp


class Questionnaire:
    
    
    def __init__(self, ftp_server, mysql_server, service_dir, root_dir, studies, data_stream, required_fields, optional_fields, constraints, table_name, timezones):
                  
        self.service_dir = service_dir
        self.tmp_file = os.path.join(self.service_dir, 'tmp_file.tmp')
        
        self.root_dir = root_dir
        self.studies = [os.path.join(self.root_dir, el) for el in studies]   
        self.data_stream = data_stream
        self.required_fields = required_fields
        self.optional_fields = optional_fields
        self.all_fields = self.required_fields.copy()
        self.all_fields.update(optional_fields)
        self.constraints = constraints
        self.table_name = table_name
        self.timezones = timezones
        
        self.log_file = dt.now().strftime('%Y-%m-%d_%Hh%Mm%Ss_') + self.data_stream + '.log'
        self.log_file = os.path.join(self.service_dir, self.log_file)
        logging.basicConfig(filename = self.log_file, filemode = 'a', level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        self.participants = []
        self.files = []
        
        #######################################################################
        
        self.ftp_conn, self.db_conn, self.db_cursor = None, None, None
        
        try:
            self.ftp_conn = pysftp.Connection(**ftp_server)
        except:
            logging.info('Failed to connect to the FTP server.')
        
        try:
            self.db_conn = mysql.connector.connect(**mysql_server)
            if self.db_conn.is_connected():
                self.db_cursor = self.db_conn.cursor(buffered = True)
        except:
            logging.info('Failed to connect to the MySQL database.')
            
    
    def get_participants(self):
        
        db_participants, ftp_participants = {}, {}
        
        try:
            self.db_cursor.execute('SELECT participant_name, site_id FROM participants')
            for participant, site in self.db_cursor:
                db_participants[participant] = site
        except:
            logging.info('Failed to retrieve participant identifiers from the MySQL database.')
        
        for el in self.studies:
            try:
                self.ftp_conn.cwd(el)
                ftp_participants[el] = self.ftp_conn.listdir()   
                if not len(ftp_participants[el]):
                    logging.info('No data files available on the FTP server in study {:s}.' . format(el))
            except:
                logging.info('Failed to retrieve participant identifiers from the FTP server in study {:s}.' . format(el))
        
        for study, participants in ftp_participants.items():
            selection = [el for el in participants if el in db_participants]
            self.participants.extend([(study, el, db_participants[el]) for el in selection])
    
    
    def get_file_descriptors(self):
        
        for study, participant, site in self.participants:
            base_dir = os.path.join(study, participant, self.data_stream)
            try:
                all_files = [os.path.join(base_dir, el) for el in self.ftp_conn.listdir(base_dir)]
                csv_files = list(filter(lambda el: el.endswith('.csv.gz') or el.endswith('.csv'), all_files))
                csv_specs = [self.ftp_conn.stat(el) for el in csv_files]
                self.files.extend([(participant, site, file_name, file_spec.st_size, file_spec.st_mtime) 
                                    for file_name, file_spec in zip(csv_files, csv_specs)])
            except:
                logging.info('Failed to retrieve file specifications for participant {:s} in study {:s}.' . format(participant, study))
        
    
    def __extract_data(self, participant, file_name, file_size, last_modified):
        
        data_table, table_fields = pandas.DataFrame(), set()
        
        try:
            query = "SELECT * FROM {:s} WHERE participant_name = '{:s}' AND file_name = '{:s}' AND file_size = {:d} AND file_modified_on = {:d}"
            query = query.format(self.table_name, participant, file_name, file_size, last_modified)
            self.db_cursor.execute(query)                
            if self.db_cursor.rowcount == 0:
                self.ftp_conn.get(file_name, self.tmp_file)
        except:
            logging.info('Failed to download data file {:s} from the FTP server.' . format(file_name))

        try:
            compression = 'gzip' if file_name.endswith('.gz') else None
            data_table = pandas.read_csv(self.tmp_file, compression = compression)
            table_fields = set(data_table.columns)    
        except:
            logging.info('Failed to extract data from file {:s}.' . format(file_name))
        
        if os.path.isfile(self.tmp_file):
            os.remove(self.tmp_file)
        
        return data_table, table_fields
    
    
    def __transform_data_structure(self, data_table, file_name):
        
        data_records = []

        try:        
            data_records = data_table.to_dict(orient = 'records')  
        except:
            logging.info('Failed to convert data in file {:s} to an iterable.' . format(file_name))
            
        return data_records


    def __is_record_valid(self, single_record, file_name):
        
        valid = False
        
        try:
            valid = all([self.constraints[key](single_record[key]) for key in self.constraints])
        except:
            logging.info('At least, one data record in file {:s} does not meet constraints.' . format(file_name))
        
        return valid   
    
    
    def __get_local_time(self, raw_timestamp, timezone, file_name):
        
        str_timestamp = ''
        
        try:
            utc_timestamp = dt.fromtimestamp(round(raw_timestamp), tz.gettz('UTC'))
            loc_timestamp = utc_timestamp.astimezone(tz.gettz(timezone))
            str_timestamp = loc_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        except:
            logging.info('Failed to convert timestamp {:f} in file {:s}.' . format(raw_timestamp, file_name))
        
        return str_timestamp


    def __get_time_interval_id(self, str_timestamp, file_name):
        
        time_interval = None
        
        try:
            query = "SELECT timeInterval_ID FROM timeIntervals WHERE datetimeStart <= '{:s}' AND '{:s}' <= datetimeEnd"
            query = query.format(str_timestamp, str_timestamp)
            self.db_cursor.execute(query)
            if self.db_cursor.rowcount == 1:
                time_interval = self.db_cursor.fetchone()[0]
            else:
                logging.info('No or multiple links to completion time {:s} in file {:s}.' . format(str_timestamp, file_name))
        except:
            logging.info('Failed to link completion time {:s} in file {:s}.' . format(str_timestamp, file_name))
                        
        return time_interval                   
                        
    
    def __form_query(self, participant, site, file_name, file_size, last_modified, single_record):
        
        query_fields = 'participant_name, file_name, file_size, file_modified_on'
        query_values = "'{:s}', '{:s}', {:d}, {:d}" . format(participant, file_name, file_size, last_modified)
                    
        for single_field in self.all_fields:
            field_name, field_value = self.all_fields[single_field], single_record[single_field] 
            query_fields += ', ' + field_name
            if not isinstance(field_value, str):
                field_value = str(field_value)
            elif field_value != 'NULL':
                field_value = "'" + field_value + "'"
            query_values += ', ' + field_value   
                    
        raw_timestamp, timezone = single_record['value.timeCompleted'], self.timezones[site]
        str_timestamp = self.__get_local_time(raw_timestamp, timezone, file_name)
        if str_timestamp:
            query_fields += ', ' + 'time_completed_local'
            query_values += ", '" + str_timestamp + "'"
                    
        time_interval = self.__get_time_interval_id(str_timestamp, file_name)
        if time_interval:
            query_fields += ', ' + 'time_interval'
            query_values += ', ' + str(time_interval)
        
        return query_fields, query_values
    
    
    def __add_new_data(self, query_fields, query_values):
        
        try:
            query = 'INSERT INTO {:s} ({:s}) VALUES ({:s})' . format(self.table_name, query_fields, query_values)
            self.db_cursor.execute(query)
            self.db_conn.commit()
        except:
            logging.info('Failed to execute SQL query {:s}.' . format(query))
    
    
    def update_db(self):
        
        required_fields = set(self.required_fields.keys())
        
        for single_file in self.files:    
                      
            participant, site, file_name, file_size, last_modified = single_file
            data_table, table_fields = self.__extract_data(participant, file_name, file_size, last_modified) 
            
            if table_fields and not required_fields.difference(table_fields.union(['value.timeNotification'])):                
                
                data_records = self.__transform_data_structure(data_table, file_name)
                        
                for single_record in data_records:
                               
                    if not self.__is_record_valid(single_record, file_name):
                        continue
                    
                    if (('value.timeNotification' in single_record and pandas.isna(single_record['value.timeNotification'])) or
                         'value.timeNotification' not in single_record):
                        single_record['value.timeNotification'] = 'NULL'                    
                    
                    query_fields, query_values = self.__form_query(participant, site, file_name, file_size, last_modified, single_record) 
                    self.__add_new_data(query_fields, query_values)
               
            else:
                logging.info('Data file {:s} does not include all required fields.' . format(file_name))            


    def __del__(self):
        
        if self.db_cursor:
            self.db_cursor.close()

        if self.db_conn and self.db_conn.is_connected():
            self.db_conn.close()
        
        if self.ftp_conn:
            self.ftp_conn.close()

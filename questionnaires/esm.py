"""
Created on: January 20, 2020.
Last modified on: February 11, 2020.
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


from questionnaire import Questionnaire
import logging
import pandas
import params


DATA_STREAM = 'questionnaire_esm'


MAPPING_SCHEMA = {'esm_sleep_well'          : 0,   'esm_cheerful'            : 1,   'esm_down'            : 2,
                  'esm_anxious'             : 3,   'esm_relaxed'             : 4,   'esm_irritated'       : 5,
                  'esm_stressed'            : 6,   'esm_content'             : 7,   'esm_insecure'        : 8,
                  'esm_hopeful'             : 9,   'esm_lonely'              : 10,  'esm_statisfied'      : 11,
                  'esm_restless'            : 12,  'esm_confident'           : 13,  'esm_qol'             : 14, 
                  'esm_ruminating'          : 15,  'esm_concentrate'         : 16,  'esm_phys_activity'   : 17,
                  'esm_activity_well'       : 18,  'esm_activity_else'       : 19,  'esm_activity_effort' : 20, 
                  'esm_physical_active'     : 21,  'esm_physical_tired'      : 22,  'esm_physical_pain'   : 23,
                  'esm_physical_well'       : 24,  'esm_location'            : 25,  'esm_social_interact' : 26,
                  'esm_social_people'       : 27,  'esm_social_doing'        : 28,  'esm_social_pleasant' : 29,
                  'esm_social_alone'        : 30,  'esm_social_connected'    : 31,  'esm_social_ok_alone' : 32,
                  'esm_social_others'       : 33,  'esm_social_choice'       : 34,  'esm_social_left_out' : 35,
                  'esm_social_virt_interact': 36,  'esm_social_virt_pleasant': 37,  'esm_stress_situation': 38,
                  'esm_stress'              : 39,  'esm_event'               : 40,  'esm_medication'      : 41,
                  'esm_beep'                : 42,  'esm_questionnaire'       : 43,}


REQUIRED_FIELDS = {'key.projectId'      : 'project_id',          'key.userId'            : 'user_id', 
                   'key.sourceId'       : 'source_id',           'value.time'            : 'time', 
                   'value.timeCompleted': 'time_completed_utc',  'value.timeNotification': 'time_notification',
                   'value.name'         : 'name',                'value.version'         : 'version',}


OPTIONAL_FIELDS = {'value.answers.0.questionId' : 'question_id0',        'value.answers.0.value'   : 'value0', 
                   'value.answers.0.startTime'  : 'start_time0',         'value.answers.0.endTime' : 'end_time0',
                   'value.answers.1.questionId' : 'question_id1',        'value.answers.1.value'   : 'value1', 
                   'value.answers.1.startTime'  : 'start_time1',         'value.answers.1.endTime' : 'end_time1',
                   'value.answers.2.questionId' : 'question_id2',        'value.answers.2.value'   : 'value2', 
                   'value.answers.2.startTime'  : 'start_time2',         'value.answers.2.endTime' : 'end_time2',
                   'value.answers.3.questionId' : 'question_id3',        'value.answers.3.value'   : 'value3', 
                   'value.answers.3.startTime'  : 'start_time3',         'value.answers.3.endTime' : 'end_time3',
                   'value.answers.4.questionId' : 'question_id4',        'value.answers.4.value'   : 'value4', 
                   'value.answers.4.startTime'  : 'start_time4',         'value.answers.4.endTime' : 'end_time4',
                   'value.answers.5.questionId' : 'question_id5',        'value.answers.5.value'   : 'value5', 
                   'value.answers.5.startTime'  : 'start_time5',         'value.answers.5.endTime' : 'end_time5',
                   'value.answers.6.questionId' : 'question_id6',        'value.answers.6.value'   : 'value6', 
                   'value.answers.6.startTime'  : 'start_time6',         'value.answers.6.endTime' : 'end_time6',
                   'value.answers.7.questionId' : 'question_id7',        'value.answers.7.value'   : 'value7', 
                   'value.answers.7.startTime'  : 'start_time7',         'value.answers.7.endTime' : 'end_time7',
                   'value.answers.8.questionId' : 'question_id8',        'value.answers.8.value'   : 'value8', 
                   'value.answers.8.startTime'  : 'start_time8',         'value.answers.8.endTime' : 'end_time8',
                   'value.answers.9.questionId' : 'question_id9',        'value.answers.9.value'   : 'value9', 
                   'value.answers.9.startTime'  : 'start_time9',         'value.answers.9.endTime' : 'end_time9',          
                   'value.answers.10.questionId': 'question_id10',       'value.answers.10.value'  : 'value10', 
                   'value.answers.10.startTime' : 'start_time10',        'value.answers.10.endTime': 'end_time10',
                   'value.answers.11.questionId': 'question_id11',       'value.answers.11.value'  : 'value11', 
                   'value.answers.11.startTime' : 'start_time11',        'value.answers.11.endTime': 'end_time11',
                   'value.answers.12.questionId': 'question_id12',       'value.answers.12.value'  : 'value12', 
                   'value.answers.12.startTime' : 'start_time12',        'value.answers.12.endTime': 'end_time12',
                   'value.answers.13.questionId': 'question_id13',       'value.answers.13.value'  : 'value13', 
                   'value.answers.13.startTime' : 'start_time13',        'value.answers.13.endTime': 'end_time13',
                   'value.answers.14.questionId': 'question_id14',       'value.answers.14.value'  : 'value14', 
                   'value.answers.14.startTime' : 'start_time14',        'value.answers.14.endTime': 'end_time14',
                   'value.answers.15.questionId': 'question_id15',       'value.answers.15.value'  : 'value15', 
                   'value.answers.15.startTime' : 'start_time15',        'value.answers.15.endTime': 'end_time15',
                   'value.answers.16.questionId': 'question_id16',       'value.answers.16.value'  : 'value16', 
                   'value.answers.16.startTime' : 'start_time16',        'value.answers.16.endTime': 'end_time16',
                   'value.answers.17.questionId': 'question_id17',       'value.answers.17.value'  : 'value17', 
                   'value.answers.17.startTime' : 'start_time17',        'value.answers.17.endTime': 'end_time17',
                   'value.answers.18.questionId': 'question_id18',       'value.answers.18.value'  : 'value18', 
                   'value.answers.18.startTime' : 'start_time18',        'value.answers.18.endTime': 'end_time18',
                   'value.answers.19.questionId': 'question_id19',       'value.answers.19.value'  : 'value19', 
                   'value.answers.19.startTime' : 'start_time19',        'value.answers.19.endTime': 'end_time19',          
                   'value.answers.20.questionId': 'question_id20',       'value.answers.20.value'  : 'value20', 
                   'value.answers.20.startTime' : 'start_time20',        'value.answers.20.endTime': 'end_time20',
                   'value.answers.21.questionId': 'question_id21',       'value.answers.21.value'  : 'value21', 
                   'value.answers.21.startTime' : 'start_time21',        'value.answers.21.endTime': 'end_time21',
                   'value.answers.22.questionId': 'question_id22',       'value.answers.22.value'  : 'value22', 
                   'value.answers.22.startTime' : 'start_time22',        'value.answers.22.endTime': 'end_time22',
                   'value.answers.23.questionId': 'question_id23',       'value.answers.23.value'  : 'value23', 
                   'value.answers.23.startTime' : 'start_time23',        'value.answers.23.endTime': 'end_time23',
                   'value.answers.24.questionId': 'question_id24',       'value.answers.24.value'  : 'value24', 
                   'value.answers.24.startTime' : 'start_time24',        'value.answers.24.endTime': 'end_time24',
                   'value.answers.25.questionId': 'question_id25',       'value.answers.25.value'  : 'value25', 
                   'value.answers.25.startTime' : 'start_time25',        'value.answers.25.endTime': 'end_time25',
                   'value.answers.26.questionId': 'question_id26',       'value.answers.26.value'  : 'value26', 
                   'value.answers.26.startTime' : 'start_time26',        'value.answers.26.endTime': 'end_time26',
                   'value.answers.27.questionId': 'question_id27',       'value.answers.27.value'  : 'value27', 
                   'value.answers.27.startTime' : 'start_time27',        'value.answers.27.endTime': 'end_time27',
                   'value.answers.28.questionId': 'question_id28',       'value.answers.28.value'  : 'value28', 
                   'value.answers.28.startTime' : 'start_time28',        'value.answers.28.endTime': 'end_time28',
                   'value.answers.29.questionId': 'question_id29',       'value.answers.29.value'  : 'value29', 
                   'value.answers.29.startTime' : 'start_time29',        'value.answers.29.endTime': 'end_time29',          
                   'value.answers.30.questionId': 'question_id30',       'value.answers.30.value'  : 'value30', 
                   'value.answers.30.startTime' : 'start_time30',        'value.answers.30.endTime': 'end_time30',
                   'value.answers.31.questionId': 'question_id31',       'value.answers.31.value'  : 'value31', 
                   'value.answers.31.startTime' : 'start_time31',        'value.answers.31.endTime': 'end_time31',
                   'value.answers.32.questionId': 'question_id32',       'value.answers.32.value'  : 'value32', 
                   'value.answers.32.startTime' : 'start_time32',        'value.answers.32.endTime': 'end_time32',
                   'value.answers.33.questionId': 'question_id33',       'value.answers.33.value'  : 'value33', 
                   'value.answers.33.startTime' : 'start_time33',        'value.answers.33.endTime': 'end_time33',
                   'value.answers.34.questionId': 'question_id34',       'value.answers.34.value'  : 'value34', 
                   'value.answers.34.startTime' : 'start_time34',        'value.answers.34.endTime': 'end_time34',
                   'value.answers.35.questionId': 'question_id35',       'value.answers.35.value'  : 'value35', 
                   'value.answers.35.startTime' : 'start_time35',        'value.answers.35.endTime': 'end_time35',
                   'value.answers.36.questionId': 'question_id36',       'value.answers.36.value'  : 'value36', 
                   'value.answers.36.startTime' : 'start_time36',        'value.answers.36.endTime': 'end_time36',
                   'value.answers.37.questionId': 'question_id37',       'value.answers.37.value'  : 'value37', 
                   'value.answers.37.startTime' : 'start_time37',        'value.answers.37.endTime': 'end_time37',
                   'value.answers.38.questionId': 'question_id38',       'value.answers.38.value'  : 'value38', 
                   'value.answers.38.startTime' : 'start_time38',        'value.answers.38.endTime': 'end_time38',
                   'value.answers.39.questionId': 'question_id39',       'value.answers.39.value'  : 'value39', 
                   'value.answers.39.startTime' : 'start_time39',        'value.answers.39.endTime': 'end_time39',          
                   'value.answers.40.questionId': 'question_id40',       'value.answers.40.value'  : 'value40', 
                   'value.answers.40.startTime' : 'start_time40',        'value.answers.40.endTime': 'end_time40',
                   'value.answers.41.questionId': 'question_id41',       'value.answers.41.value'  : 'value41', 
                   'value.answers.41.startTime' : 'start_time41',        'value.answers.41.endTime': 'end_time41',
                   'value.answers.42.questionId': 'question_id42',       'value.answers.42.value'  : 'value42', 
                   'value.answers.42.startTime' : 'start_time42',        'value.answers.42.endTime': 'end_time42',
                   'value.answers.43.questionId': 'question_id43',       'value.answers.43.value'  : 'value43', 
                   'value.answers.43.startTime' : 'start_time43',        'value.answers.43.endTime': 'end_time43',}


TABLE_NAME = 'esm_data'


TIME_RANGE = lambda x: x == 'NULL' if isinstance(x, str) else 1514764799 <= x <= 1640995199 # Between 2017-12-31 11:59:59 PM and 2021-12-31 11:59:59 PM


CONSTRAINTS = {'value.time': TIME_RANGE, 'value.timeCompleted': TIME_RANGE,}


TIMEZONES = {0: 'UTC',
             1: 'Europe/Madrid',
             2: 'Europe/Madrid',
             3: 'Europe/London',
             4: 'Europe/Rome',
             5: 'Europe/Copenhagen',
             6: 'Europe/Madrid',
             7: 'Europe/Amsterdam',}


class ESM_Questionnaire(Questionnaire):
    
    
    def set_mapping_schema(self, mapping_schema):
    
        self.mapping_schema = mapping_schema   
        self.n_schema_elements = len(mapping_schema)
        
        self.empty_record = {}
        for key in self.all_fields:
            self.empty_record[key] = 'NULL'
        for key, value in self.mapping_schema.items():
            field_name = 'value.answers.{:d}.questionId' . format(value)
            self.empty_record[field_name] = key
      
    
    def _Questionnaire__transform_data_structure(self, data_table, file_name):
        
        data_records = []
                
        name_templates = ['value.answers.{:d}.questionId', 
                          'value.answers.{:d}.value',
                          'value.answers.{:d}.startTime',  
                          'value.answers.{:d}.endTime',]
        
        try:
            
            for _, single_record in data_table.iterrows():
                
                tmp_record = self.empty_record.copy()  
                
                for field_name in self.required_fields:
                    tmp_record[field_name] = single_record[field_name] if field_name in single_record else 'NULL'
                
                for field_counter in range(self.n_schema_elements):
                    param_names   = list(map(lambda x: x . format(field_counter), name_templates))
                    param_values  = list(map(lambda x: single_record[x] if x in single_record else 'NULL', param_names))
                    question_name = param_values[0]
                    if question_name in self.mapping_schema:
                        index = self.mapping_schema[question_name]
                        adjusted_names = list(map(lambda x: x . format(index), name_templates))
                        for field_name, field_value in zip(adjusted_names, param_values):
                            tmp_record[field_name] = 'NULL' if pandas.isna(field_value) else field_value
                            
                data_records.append(tmp_record)
                
        except:
            
            logging.info('Failed to convert data in file {:s} to an iterable.' . format(file_name))

            
        return data_records


if __name__ == '__main__':
    
    print('Step #1')
    q = ESM_Questionnaire(params.FTP_SERVER, 
                          params.MYSQL_SERVER, 
                          params.SERVICE_DIR, 
                          params.ROOT_DIR, 
                          params.STUDIES, 
                          DATA_STREAM, 
                          REQUIRED_FIELDS, 
                          OPTIONAL_FIELDS,
                          CONSTRAINTS,
                          TABLE_NAME,
                          TIMEZONES)
    q.set_mapping_schema(MAPPING_SCHEMA)
    
    print('Step #2')
    q.get_participants()
    
    print('Step #3')
    q.get_file_descriptors()
    
    print('Step #4')
    q.update_db()
    
    print('Step #5')
    del q
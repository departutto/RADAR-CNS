"""
Created on: January 17, 2020.
Last modified on: January 19, 2020.
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

import questionnaire
import params


DATA_STREAM = 'questionnaire_rses'


FIELDS = {'key.projectId'             : 'project_id',         'key.userId'             : 'user_id', 
          'key.sourceId'              : 'source_id',          'value.time'             : 'time', 
          'value.timeCompleted'       : 'time_completed_utc', 'value.timeNotification' : 'time_notification', 
          'value.name'                : 'name',               'value.version'          : 'version', 
          'value.answers.0.questionId': 'question_id0',       'value.answers.0.value'  : 'value0', 
          'value.answers.0.startTime' : 'start_time0',        'value.answers.0.endTime': 'end_time0',
          'value.answers.1.questionId': 'question_id1',       'value.answers.1.value'  : 'value1', 
          'value.answers.1.startTime' : 'start_time1',        'value.answers.1.endTime': 'end_time1',
          'value.answers.2.questionId': 'question_id2',       'value.answers.2.value'  : 'value2', 
          'value.answers.2.startTime' : 'start_time2',        'value.answers.2.endTime': 'end_time2',
          'value.answers.3.questionId': 'question_id3',       'value.answers.3.value'  : 'value3', 
          'value.answers.3.startTime' : 'start_time3',        'value.answers.3.endTime': 'end_time3',
          'value.answers.4.questionId': 'question_id4',       'value.answers.4.value'  : 'value4', 
          'value.answers.4.startTime' : 'start_time4',        'value.answers.4.endTime': 'end_time4',
          'value.answers.5.questionId': 'question_id5',       'value.answers.5.value'  : 'value5', 
          'value.answers.5.startTime' : 'start_time5',        'value.answers.5.endTime': 'end_time5',
          'value.answers.6.questionId':'question_id6',        'value.answers.6.value'  : 'value6', 
          'value.answers.6.startTime' : 'start_time6',        'value.answers.6.endTime': 'end_time6',
          'value.answers.7.questionId': 'question_id7',       'value.answers.7.value'  : 'value7', 
          'value.answers.7.startTime' : 'start_time7',        'value.answers.7.endTime': 'end_time7',
          'value.answers.8.questionId': 'question_id8',       'value.answers.8.value'  : 'value8', 
          'value.answers.8.startTime' : 'start_time8',        'value.answers.8.endTime': 'end_time8',
          'value.answers.9.questionId': 'question_id9',       'value.answers.9.value'  : 'value9', 
          'value.answers.9.startTime' : 'start_time9',        'value.answers.9.endTime': 'end_time9',}


TABLE_NAME = 'rses_data'


TIME_RANGE = lambda x: 1514764799 <= x <= 1640995199 # Between 2017-12-31 11:59:59 PM and 2021-12-31 11:59:59 PM


ANSWERS_VALUES = lambda x: x in set([0, 1, 2, 3])


CONSTRAINTS = {'value.time'                : TIME_RANGE,               'value.timeCompleted'    : TIME_RANGE,
               'value.answers.0.questionId': lambda x: x == 'rses_1',  'value.answers.0.value'  : ANSWERS_VALUES,
               'value.answers.0.startTime' : TIME_RANGE,               'value.answers.0.endTime': TIME_RANGE,
               'value.answers.1.questionId': lambda x: x == 'rses_2',  'value.answers.1.value'  : ANSWERS_VALUES,
               'value.answers.1.startTime' : TIME_RANGE,               'value.answers.1.endTime': TIME_RANGE,
               'value.answers.2.questionId': lambda x: x == 'rses_3',  'value.answers.2.value'  : ANSWERS_VALUES,
               'value.answers.2.startTime' : TIME_RANGE,               'value.answers.2.endTime': TIME_RANGE,
               'value.answers.3.questionId': lambda x: x == 'rses_4',  'value.answers.3.value'  : ANSWERS_VALUES,
               'value.answers.3.startTime' : TIME_RANGE,               'value.answers.3.endTime': TIME_RANGE,
               'value.answers.4.questionId': lambda x: x == 'rses_5',  'value.answers.4.value'  : ANSWERS_VALUES,
               'value.answers.4.startTime' : TIME_RANGE,               'value.answers.4.endTime': TIME_RANGE,
               'value.answers.5.questionId': lambda x: x == 'rses_6',  'value.answers.5.value'  : ANSWERS_VALUES,
               'value.answers.5.startTime' : TIME_RANGE,               'value.answers.5.endTime': TIME_RANGE,
               'value.answers.6.questionId': lambda x: x == 'rses_7',  'value.answers.6.value'  : ANSWERS_VALUES,
               'value.answers.6.startTime' : TIME_RANGE,               'value.answers.6.endTime': TIME_RANGE,
               'value.answers.7.questionId': lambda x: x == 'rses_8',  'value.answers.7.value'  : ANSWERS_VALUES,
               'value.answers.7.startTime' : TIME_RANGE,               'value.answers.7.endTime': TIME_RANGE,               
               'value.answers.8.questionId': lambda x: x == 'rses_9',  'value.answers.8.value'  : ANSWERS_VALUES,
               'value.answers.8.startTime' : TIME_RANGE,               'value.answers.8.endTime': TIME_RANGE,               
               'value.answers.9.questionId': lambda x: x == 'rses_10', 'value.answers.9.value'  : ANSWERS_VALUES,
               'value.answers.9.startTime' : TIME_RANGE,               'value.answers.9.endTime': TIME_RANGE,}


TIMEZONES = {0: 'UTC',
             1: 'Europe/Madrid',
             2: 'Europe/Madrid',
             3: 'Europe/London',
             4: 'Europe/Rome',
             5: 'Europe/Copenhagen',
             6: 'Europe/Madrid',
             7: 'Europe/Amsterdam',}


if __name__ == '__main__':
    
    print('Step #1')
    q = questionnaire.Questionnaire(params.FTP_SERVER, 
                                    params.MYSQL_SERVER, 
                                    params.SERVICE_DIR, 
                                    params.ROOT_DIR, 
                                    params.STUDIES, 
                                    DATA_STREAM, 
                                    FIELDS, 
                                    CONSTRAINTS,
                                    TABLE_NAME,
                                    TIMEZONES)
    
    print('Step #2')
    q.get_participants()
    
    print('Step #3')
    q.get_file_descriptors()
    
    print('Step #4')
    q.update_db()
    
    print('Step #5')
    del q
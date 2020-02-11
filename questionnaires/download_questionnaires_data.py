"""
Created on: February 11, 2020.
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


import csv
import params
import mysql.connector


QUESTIONNAIRES = [('SELECT * FROM phq8_data', '/home/ubuntu/Dima/downloaded_data/phq8.csv'),
                  ('SELECT * FROM rses_data', '/home/ubuntu/Dima/downloaded_data/rses.csv'),
                  ('SELECT * FROM esm_data',  '/home/ubuntu/Dima/downloaded_data/esm.csv'),]


if __name__ == '__main__':
    
    try:
        
        db_conn = mysql.connector.connect(**params.MYSQL_SERVER)
        if db_conn.is_connected():
            db_cursor = db_conn.cursor()
        
        for query, pathname in QUESTIONNAIRES:
            
            db_cursor.execute(query)
            columns = [el[0] for el in db_cursor.description]
            rows = db_cursor.fetchall()
        
            raw_file = open(pathname, 'w')
            csv_file = csv.writer(raw_file)
            csv_file.writerow(columns)
            csv_file.writerows(rows)
            raw_file.close()      
            
    except Exception as exc:
        
        print(exc)
    
    finally:
        
        if db_cursor:
            db_cursor.close()

        if db_conn and db_conn.is_connected():
            db_conn.close()
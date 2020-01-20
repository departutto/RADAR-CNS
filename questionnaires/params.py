"""
Created on: January 17, 2020.
Last modified on: January 20, 2020.
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


FTP_SERVER = {'host'       : None,
              'username'   : None,
              'private_key': None,}


MYSQL_SERVER = {'host'    : None,
                'database': None,
                'user'    : None,
                'password': None,}


SERVICE_DIR = None


ROOT_DIR = None


STUDIES = set(['RADAR-MDD-CIBER-S1',
               'RADAR-MDD-CIBER-s1',
               'RADAR-MDD-IISPV-s1',
               'RADAR-MDD-KCL-s1',
               'RADAR-MDD-VUmc-s1',
               'RADAR-MSDep-RegionH-s1',
               'RADAR-MSDIS-REGIONH-S1',
               'RADAR-MSDis-RegionH-s1',
               'RADAR-MSDep-OSR-s1',
               'RADAR-MSDis-OSR-s1',
               'RADAR-MSDep-VHIR',
               'RADAR-MSDep-VHIR-s1',
               'RADAR-MSDis-VHIR',
               'RADAR-MSDis-VHIR-s1',])

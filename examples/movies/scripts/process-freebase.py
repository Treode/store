#!/usr/bin/env python
# Copyright 2014 Treode, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" 
This script process a full RDF dump and filters it to create three files with much reduced content.
After the filtered files have been created, this script may upload into a treode movies server.

filter command
requires:
 - a full RDF dump from freebase in gzip format.
produces:
 - film.filtered.data.rdf -> has all film data we need for uploading

Example:
time process-freebase.py filter <freebase-rdf-gz-file>

Upload command
requires:
 - film.filtered.data.rdf  (created by the filter command)

Example:
time ./process-freebase.py upload --filtered=film.filtered.data.rdf

RDF:

Films in RDF dump are in this format:
<http://rdf.freebase.com/ns/m.0yq437z><http://rdf.freebase.com/ns/type.object.type><http://rdf.freebase.com/ns/film.film>.

Performances in RDF dump are in this format:
<http://rdf.freebase.com/ns/m.02nwvmx><http://rdf.freebase.com/ns/film.performance.film><http://rdf.freebase.com/ns/m.0ddjy>.
<http://rdf.freebase.com/ns/m.02nwvmx><http://rdf.freebase.com/ns/film.performance.actor><http://rdf.freebase.com/ns/m.0c0k1>.
<http://rdf.freebase.com/ns/m.02nwvmx><http://rdf.freebase.com/ns/film.performance.character><http://rdf.freebase.com/ns/m.0fjn8>.

"""
from optparse import OptionParser
import sys
import gzip
import re
import httplib
from json import JSONEncoder
import time, datetime
import logging
import json


# REGEX
#
# names
re_objnames = re.compile(r'''<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/type.object.name>\s+((?<![\\])['"])((?:.(?!(?<![\\])\1))*.?)\2@en\s+\.\s*$''')
#
# films
re_films = re.compile(r'^<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/type.object.type>\s+<http://rdf\.freebase\.com/ns/film\.film>\s+\.$')
#
# performance: actor, character and film
re_filmdata = re.compile(r'^<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/film\.performance\.(film|actor|character)>\s+<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+\.$')

# films - for future use
re_filmobj = re.compile(r'^<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/film\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+\.$')


class MoviesRDFDumpParser:

    def __init__(self, options):
        self.options = options
        FORMAT = '%(asctime)-15s %(name)s[%(process)d] [%(levelname)s] %(message)s'
        logging.basicConfig(format=FORMAT, filename="upload.log", level=logging.DEBUG)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        if options.console:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            console.setFormatter(logging.Formatter(FORMAT))
            self.logger.addHandler(console)

    def upload_filmname(self, movie_id, movie_title):
        try:
            data = JSONEncoder().encode({"id": movie_id, "title": movie_title})
            start_time = time.time()
            conn = httplib.HTTPConnection(self.options.host, self.options.port)
            conn.request("PUT", "/movie/%s" % movie_id, data, headers={"Connection": "close"})
            response = conn.getresponse()
            elapsed_time = time.time() - start_time
            response.read()
            conn.close()
            self.logger.debug("response=%d,elapsed_ms=%s,DATA=%s" % (response.status,
                                                                     str(datetime.timedelta(seconds=elapsed_time)),
                                                                     data))
        except:
            self.logger.error("uploading %s" % str(data))
            # self.logger.exception(movie_id)


    def upload_filmdata(self, _data):
        movie_c = httplib.HTTPConnection(self.options.host, self.options.port)
        movie_c.request("GET", "/movie/%s" % _data['film'], headers={"Connection": "close"})
        movie_r = movie_c.getresponse()
        movie_r.read()
        movie_c.close()
        if movie_r.status == 200:
            role_name = "?"  # default role name
            role_id = _data.setdefault('character', "?")
            role_name = self._names.setdefault(role_id, "?")
            data_dict ={"id": _data['actor'],
                        "name": self._names.setdefault(_data['actor'], "?"),
                        "roles": [{"movieId": _data['film'],
                                   "role": role_name}]}
            actor_c = httplib.HTTPConnection(self.options.host, self.options.port)
            actor_c.request("GET","/actor/%s" % _data['actor'], headers={"Connection": "close"})
            actor_r = actor_c.getresponse()
            actor_data = actor_r.read()
            actor_r.close()
            actor_c.close()
            if actor_r.status == 200:
                stored_data = json.loads(actor_data)
                if 'roles' in stored_data:
                    data_dict['roles'] = data_dict['roles'] + stored_data['roles']

            data = JSONEncoder().encode(data_dict)
            
            c = httplib.HTTPConnection(self.options.host, self.options.port)
            c.request("PUT", "/actor/%s" % _data['actor'], data, headers={"Connection": "close"})
            r = c.getresponse()
            d = r.read()
            self.logger.info("code:%d: %s" % (r.status,str(data)))
            self.logger.debug("STATUS: %d, REASON: %s, HEADERS: %s, DATA: :%s" % (r.status,
                                                                                  r.reason,
                                                                                  r.getheaders(),
                                                                                  str(data)))
            c.close()
                
        else:
            self.logger.error("MOVIE NOT FOUND %d -  %s" % (movie_r.status, str(_data)))

    def upload_filteredfile(self):
        self._names = {}
        self._films = []
        self._film_data = {}
        start_time = time.time()
        self.logger.info("Start processing filtered data...")
        with open(self.options.filtered, 'rb') as f_filtered:
            for line in f_filtered:
                # NAMES
                m = re_objnames.match(line)
                if m is not None:
                    self._names[m.group(1)] = m.group(3)
                else:
                    # FILMS
                    m = re_films.match(line)
                    if m is not None:
                        self._films.append(m.group(1))
                    else:
                        # FILM DATA
                        m = re_filmdata.match(line)
                        if m is not None:
                            self._film_data.setdefault(m.group(1), {})[m.group(2)] = m.group(3)
        self.logger.info("Data in memory")

        if not self.options.notitles:
            self.logger.info("Uploading films")
            for f in self._films:
                if f in self._names:
                    self.upload_filmname(f, self._names[f])
                else:
                    self.logger.info("WARNING: Film name not in English %s" % f)

        for k in self._film_data.keys():
            if 'film' in self._film_data[k] and 'actor' in self._film_data[k]:
                if self._film_data[k]['film'] in self._names and \
                   self._film_data[k]['actor'] in self._names:
                    self.upload_filmdata(self._film_data[k])
                else:
                    self.logger.info("WARNING: film data name not in english %s" % str(self._film_data[k]))
            else:
                self.logger.info("WARNING: performance data incomplete %s" % str(self._film_data[k]))

        elapsed_time = time.time() - start_time
        self.logger.info("Finished process-freebase  - elapsed %s" % (str(datetime.timedelta(seconds=elapsed_time))))

    def filter_rdfdump(self, dumpfile):
        NAMES_TMP_RDF = "names.tmp.rdf"
        self.logger.info("Processing RDF dump - Pass 1 - Films")
        f_namestmp = open('names.tmp.rdf', 'wb')
        f_filmsdata = open("film.filtered.data.rdf", 'wb')
        if options.dumpKeys:
            f_keys = open("keys.rdf", "wb")
        all_keys_set = set()
        with gzip.open(dumpfile, 'rb') as f_in:
            for line in f_in:
                # NAMES
                m = re_objnames.match(line)
                if m is not None:
                    f_namestmp.write(line)
                else:
                    # FILMS
                    m = re_films.match(line)
                    if m is not None:
                        f_filmsdata.write(line)
                        if options.dumpKeys:
                            f_keys.write(m.group(1)+"\n")
                        all_keys_set.add(m.group(1))
                    else:
                        # FILM DATA
                        m = re_filmdata.match(line)
                        if m is not None:
                            f_filmsdata.write(line)
                            all_keys_set.add(m.group(1))
                            all_keys_set.add(m.group(3))

        f_namestmp.close()
        if options.dumpKeys:
            f_keys.close()
        self.logger.info("Pass 1 - Done")
        self.logger.info("Processing RDF dump - Pass 2 - Names")

        with open(NAMES_TMP_RDF, 'rb') as f_in:
            for line in f_in:
                m = re_objnames.match(line)
                # FILM DATA
                if m is not None:
                    if m.group(1) in all_keys_set:
                        f_filmsdata.write(line)

        f_filmsdata.close()
        self.logger.info("Pass 2 - Done")
        if not options.keepTempFiles:
            os.remove(NAMES_TMP_RDF)


def main():
    usage = """Usage: %prog [filter|upload] [options]

    Commands:
    filter - filters the RDF file
    upload - uploads the filtered RDF file"""

    parser = OptionParser(usage)

    parser.add_option("--notitles",
                      dest="notitles",
                      action="store_true",
                      default=False)
    parser.add_option("--console",
                      dest="console",
                      action="store_true",
                      default=False)
    parser.add_option("--filtered",
                      dest="filtered",
                      default=None,
                      help="Filtered file")
    parser.add_option("--dumpKeys",
                      dest="dumpKeys",
                      default=False,
                      action="store_true",
                      help="Dump all keys in a file")
    parser.add_option("--keepTempFiles",
                      dest="keepTempFiles",
                      default=False,
                      action="store_true",
                      help="Keep temporary files")
    parser.add_option("--host",
                      dest="host",
                      default="127.0.0.1",
                      help="Treode movies service hostname")
    parser.add_option("--port",
                      dest="port",
                      default=7070,
                      help="Treode movies service port")

    (options, args) = parser.parse_args()

    if args and args[0] == 'upload' and options.filtered:
        dump = MoviesRDFDumpParser(options)
        dump.upload_filteredfile()
    elif args and args[0] == 'filter' and len(args) == 2:
        dump = MoviesRDFDumpParser(options)
        dump.filter_rdfdump(args[1])
    else:
        parser.print_help()

if __name__ == '__main__':
    main()

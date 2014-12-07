#!/usr/bin/env python
""" 
This script process a full RDF dump and filters it to create three files with much reduced content.
After the filtered files have been created, this script may upload into a treode movies server.

filter command
requires:
 - a full RDF dump from freebase in gzip format.
produces:
 - films.only.rdf -> has only the film.film entries
 - film.filtered.data.rdf -> has all film data we need for uploading
 - film.filtered.names.rdf -> has all RDF 'names' 
 - keys.rdf -> has all movie keys found for verification with treode.

Example:
time process-freebase.py filter <freebase-rdf-gz-file>

upload command
requires:
 - film.filtered.data.rdf
 - film.filtered.names.rdf

Example:
time ./process-freebase.py upload --film=films.only.rdf --filmperformance=film.filtered.data.rdf --names=film.filtered.names.rdf 

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

FORMAT = '%(asctime)-15s %(name)s[%(process)d] [%(levelname)s] %(message)s'
logging.basicConfig(format=FORMAT,filename="upload.log")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# REGEX
#
# names
re_objnames = re.compile ( r'''<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/type.object.name>\s+((?<![\\])['"])((?:.(?!(?<![\\])\1))*.?)\2@en\s+\.\s*$''' )
#
# films
re_films = re.compile ( r'^<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/type.object.type>\s+<http://rdf\.freebase\.com/ns/film\.film>\s+\.$' )
#
# performance: actor, character and film
re_filmdata = re.compile  (r'^<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/film\.performance\.(film|actor|character)>\s+<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+\.$' )

# films - for future use
re_filmobj = re.compile  (r'^<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/film\.([^>]+)>\s+<http://rdf\.freebase\.com/ns/m\.([^>]+)>\s+\.$' )

class MoviesRDFDumpParser:

    def upload_splitfiles ( self, options ):
        dict_names = {}
        cnt_linesprocessed = 0
        cnt_linesfilm = 0
        cnt_actors = 0
        if options.objnames:
            logger.info("Loading names in memory...")
            print "Loading names..."
            start_time = time.time()
            with open ( options.objnames, 'rb' ) as f_names:
                cnt_names = 0
                for line in f_names:
                    m = re_objnames.match(line)
                    if m is not None:
                        dict_names[ m.group(1) ] = m.group(3)
                        cnt_names = cnt_names + 1
  
            elapsed_time = time.time() - start_time
            print "Finished loading %d names - elapsed %s" % (cnt_names,str(datetime.timedelta(seconds=elapsed_time)) )
            logger.info("%d names loaded in memory - elapsed %s" % (cnt_names, str(datetime.timedelta(seconds=elapsed_time))))
        
        #
        # films in RDF dump are in the format:
        # <http://rdf.freebase.com/ns/m.0yq437z><http://rdf.freebase.com/ns/type.object.type><http://rdf.freebase.com/ns/film.film>.
        #
        if options.film:
            with open(options.film,'rb') as f_films:
                for line in f_films:
                    cnt_linesprocessed = cnt_linesprocessed + 1
                    m = re_films.match(line)
                    if m is not None:
                        if m.group(1) in dict_names:
                            try:
                                movie_id = m.group(1)
                                movie_title = dict_names[m.group(1)]
                                data = JSONEncoder().encode({"id":movie_id,"title":movie_title})
                                start_time = time.time()
                                conn = httplib.HTTPConnection(options.host,options.port)
                                conn.request("PUT","/movie/%s" % movie_id, data)
                                response = conn.getresponse()
                                elapsed_time = time.time() - start_time
                                response.read()
                                conn.close()
                                logger.info("PROCESSED=%d,FILMS=%d,ELAPSED_MS=%s,RESPONSE=%d,DATA=%s" % ( cnt_linesprocessed,
                                                                                                          cnt_linesfilm,
                                                                                                          str(datetime.timedelta(seconds=elapsed_time)),
                                                                                                          response.status,
                                                                                                          data ) )
                            except:
                                logger.exception(movie_id)
                        else:
                            logger.warn("FILM WITHOUT A NAME IN ENGLISH=%s",m.group(1))
                            

        if options.performance:

            #<http://rdf.freebase.com/ns/m.02nwvmx><http://rdf.freebase.com/ns/film.performance.film><http://rdf.freebase.com/ns/m.0ddjy>.
            #<http://rdf.freebase.com/ns/m.02nwvmx><http://rdf.freebase.com/ns/film.performance.actor><http://rdf.freebase.com/ns/m.0c0k1>.
            #<http://rdf.freebase.com/ns/m.02nwvmx><http://rdf.freebase.com/ns/film.performance.character><http://rdf.freebase.com/ns/m.0fjn8>.
            perf_dict = {}
            perf_id = ""
            cnt = 1
            with open ( options.performance, 'rb' ) as f_filmdata:
                for line in f_filmdata:
                    m = re_filmdata.match(line)
                    if m is not None:
                        if m.group(1) != perf_id:
                            if perf_dict:
                                cnt = cnt + 1
                                logger.debug("PERFORMANCE:%s" % str(perf_dict))
                                if 'film_id' in perf_dict:
                                    try:
                                        movie_c = httplib.HTTPConnection(options.host,options.port)
                                        movie_c.request("GET","/movie/%s" % perf_dict['film_id'],headers={"Connection":"close"})
                                        movie_r = movie_c.getresponse()
                                        movie_r.read()
                                        movie_c.close()
                                        if movie_r.status==200:
                                            if 'actor_id' in perf_dict:
                                                actor_name="?"
                                                role_name="?"
                                                if perf_dict['actor_id'] in dict_names:
                                                    actor_name = dict_names[perf_dict['actor_id']]
                                                if 'role_id' in perf_dict and perf_dict['role_id'] in dict_names:
                                                    role_name = dict_names[perf_dict['role_id']]
                                                data = JSONEncoder().encode({"id":perf_dict['actor_id'],
                                                                             "name":actor_name,
                                                                             "roles":[{"movieId":perf_dict['film_id'],
                                                                                       "role":role_name}]})
                                                c = httplib.HTTPConnection(options.host,options.port)
                                                c.request("PUT","/actor/%s" % perf_dict['actor_id'], data,headers={"Connection":"close"})
                                                r = c.getresponse()
                                                d = r.read()
                                                logger.debug("STATUS: %d, REASON: %s, HEADERS: %s, CONTENT:%s" % ( r.status,
                                                                                                                   r.reason,
                                                                                                                   r.getheaders(),
                                                                                                                   d ))
                                                c.close()
                                                logger.info("%d - perf_id:%s - perf_dict:%s" % (r.status,perf_id,str(perf_dict)))
                                            else:
                                                logger.error("actor_id not in perf_id:%s - perf_dict:%s" % (perf_id,str(perf_dict)))
                                        else:
                                            logger.error("MOVIE NOT FOUND %d - perf_id:%s -  %s" % ( movie_r.status,perf_id,str(perf_dict)))
                                    except:
                                        logger.exception(perf_dict)

                                else:
                                    logger.error("film_id not in perf_id (%s)  %s" % (perf_id,str(perf_dict)))
                                    
                                # cleaning perf_dict
                                perf_dict = {}

                            # set perf_id
                            perf_id = m.group(1)
                        else:
                            # same performance, store film, actor and character
                            if m.group(2) == 'film':
                                perf_dict['film_id'] = m.group(3)
                            elif m.group(2) == 'actor':
                                perf_dict['actor_id'] = m.group(3)
                            elif m.group(2) == 'character':
                                perf_dict['role_id'] = m.group(3)


    def filter_dump (self, dumpfile, options ):
        logger.info("Processing RDF dump - Pass 1 - Films")
        f_filmsdata = open ( "film.filtered.data.rdf", 'wb' )
        f_filmsonly = open ( "films.only.rdf", 'wb')
        f_keys = open ("keys.rdf" , "wb" )
        all_keys_set = set()
        with gzip.open ( dumpfile, 'rb' ) as f_in:
            for line in f_in:
                # FILMS
                m = re_films.match(line)
                if m is not None:
                    f_filmsonly.write(line)
                    f_keys.write(m.group(1)+"\n")
                    all_keys_set.add(m.group(1))
                else:
                    # FILM DATA
                    m = re_filmdata.match(line)
                    if m is not None:
                        f_filmsdata.write(line)
                        all_keys_set.add(m.group(1))
                        all_keys_set.add(m.group(3))
        f_filmsdata.close()
        f_filmsonly.close()
        f_keys.close()
        logger.info("Pass 1 - Done")
        logger.info("Processing RDF dump - Pass 2 - Names")
        f_names = open ("film.filtered.names.rdf" , 'wb' )
        with gzip.open ( dumpfile, 'rb' ) as f_in:
            for line in f_in:
                m = re_objnames.match(line)
                # FILM DATA
                if m is not None:
                    if m.group(1) in all_keys_set:
                        f_names.write(line)

        f_names.close()
        logger.info("Pass 2 - Done")

def main():
    parser = OptionParser()
    parser.add_option("--names",
                      dest="objnames",
                      default=None,
                      help="File to store object names")
    parser.add_option("--film",
                      dest="film",
                      default=None,
                      help="File to store films")
    parser.add_option("--filmperformance",
                      dest="performance",
                      default=None,
                      help="File to store film data")
    parser.add_option("--host",
                      dest="host",
                      default="127.0.0.1",
                      help="Treode movies service hostname")
    parser.add_option("--port",
                      dest="port",
                      default=7070,
                      help="Treode movies service port")

    (options,args) = parser.parse_args()

    if args[0] == 'upload' and ( options.objnames or options.film or options.performance ):
        dump = MoviesRDFDumpParser()
        dump.upload_splitfiles ( options )

    if args[0] == 'filter' and len(args) == 2:
        dump = MoviesRDFDumpParser()
        dump.filter_dump ( args[1], options )

        
if __name__ == '__main__':
    main()

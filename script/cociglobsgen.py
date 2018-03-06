import os
from argparse import ArgumentParser
import json
import requests
from requests.exceptions import ReadTimeout, ConnectTimeout
import sys
import urllib.parse
import csv
import re
import errno
import datetime
from time import sleep
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import citation

class CociprocessGlob:

    def __init__(self):
        self.CROSSREF_CODE = '020'
        self.LOOKUP_CSV = 'lookup.csv'
        self.INDEX_DATE_CSVPATH = 'index/'
        self.lookup_code = 0
        self.lookup_dic = {}
        self.date_dic = {}

    def check_make_dirs(self,filename) :
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

    #create new file with header
    def init_csv(self,csv_path,header):
        self.check_make_dirs(csv_path)
        if not os.path.isfile(csv_path):
            with open(csv_path, 'w') as csvfile:
                csvfile.write(header)

    #write on a csv_path file a given block_txt
    def write_txtblock_on_csv(self,csv_path, block_txt):
        self.check_make_dirs(csv_path)
        with open(csv_path, 'a', newline='') as csvfile:
            csvfile.write(block_txt)

    #init the lookup_dic by the contents of its corresponding csv
    def init_lookup_dic(self):
        with open(self.LOOKUP_CSV,'r') as lookupcsv:
            lookupcsv_reader = csv.DictReader(lookupcsv)
            code = -1
            for row in lookupcsv_reader:
                self.lookup_dic[row['c']] = row['code']
                code = int(row['code'])
            #last code used
            self.lookup_code = code


    #update lookup dictionary and update its corresponding csv
    def update_lookup(self,c):
        if c not in self.lookup_dic:
            #define the code following the 9 rule ...
            self.calc_next_lookup_code()
            code = self.lookup_code
            self.lookup_dic[c] = code
            self.write_txtblock_on_csv(self.LOOKUP_CSV, '\n"%s","%s"'%(c,code))


    def update_date(self,date_val, doi_key):
        if (doi_key not in self.date_dic) or (self.date_dic[doi_key] == "" and date_val != ""):
            self.date_dic[doi_key] = date_val
            self.write_txtblock_on_csv(self.INDEX_DATE_CSVPATH, '\n"%s",%s'%(self.escape_inner_quotes(doi_key),date_val))

    def escape_inner_quotes(self,str_val):
        return str_val.replace('"', '""')

    ###############  Convert CrossRef DOI to CI
    def calc_next_lookup_code(self):
        rem = self.lookup_code % 100
        newcode = self.lookup_code + 1
        if (rem==89):
            newcode = newcode * 10
        self.lookup_code = newcode

    #convert a crossref doi into a citation identifier
    def convert_doi_to_ci(self,doi_str):
        return self.CROSSREF_CODE + self.match_str_to_lookup(doi_str)

    #convert a giving string in its corresponding ci format
    #using the lookup file
    def match_str_to_lookup(self,str_val):
        ci_str = ""
        str_noprefix = str_val[3:]
        for c in str_noprefix:
            if c not in self.lookup_dic:
                self.update_lookup(c)
            ci_str = ci_str + str(self.lookup_dic[c])
        return ci_str


    def init_dirs_skeleton(self):
        self.check_make_dirs(self.INDEX_DATE_CSVPATH)
        self.INDEX_DATE_CSVPATH = "%sdate.csv"%(self.INDEX_DATE_CSVPATH)
        self.init_csv(self.INDEX_DATE_CSVPATH,'id,value')


    def build_pubdate(self,obj, doi_val):

        if doi_val in self.date_dic:
            return self.date_dic[doi_val]

        if 'issued' in obj:
            if 'date-parts' in obj['issued']:
                #is an array of parts of dates
                try:
                    obj_date = obj['issued']['date-parts'][0]

                    #lisdate[year,month,day]
                    listdate = [1,1,1]
                    dateparts = []
                    for i in range(0,len(obj_date)):
                        try:
                            dateparts.append(obj_date[i])
                            intvalue = int(obj_date[i])
                            listdate[i] = intvalue
                        except:
                            pass

                    #I have a date , so generate it
                    if (listdate[0] > 1) and (listdate[0] < 10000) and (listdate[1] > 0) and (listdate[1] <= 12) and (listdate[2] > 0) and (listdate[2] <= 31):
                        date_val = datetime.date(listdate[0], listdate[1], listdate[2])
                        dformat = '%Y'

                        #only month is specified
                        if len(dateparts) == 2 :
                            dformat = '%Y-%m'
                        else:
                            if len(dateparts) == 3 and (dateparts[1] != 1 or (dateparts[1] == 1 and dateparts[2] != 1)):
                                dformat = '%Y-%m-%d'

                        date_in_str = date_val.strftime(dformat)
                        return date_in_str

                except:
                    pass
        return ""



    def process_item(self,obj):
        if (("DOI" in obj) and ("reference" in obj)):
            citing_doi = obj["DOI"].lower()
            self.convert_doi_to_ci(citing_doi)
            citing_date = self.build_pubdate(obj,citing_doi)
            self.update_date(citing_date, citing_doi)

            for ref_item in obj['reference']:
                if "DOI" in ref_item:
                    cited_doi = ref_item["DOI"].lower()
                    self.convert_doi_to_ci(cited_doi)
        else:
            return -1


    def genGlobs(self,obj):
        if 'message' in obj:
            if 'items' in obj['message']:
                list_of_items = obj['message']['items']
                for item in list_of_items:
                    self.process_item(item)
        else:
            return -1

if __name__ == "__main__":
    arg_parser = ArgumentParser("cociprocess_glob.py", description="Process a crossref JSON files and create global index.")

    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The directory where of crossref data dump.")
    arg_parser.add_argument("-out", "--output_dir", dest="output_dir", required=False, help="The directory where the output and processing results are stored.")
    arg_parser.add_argument("-lookup", dest="lookup_file_path", required=True, help="The lookup file full path (with file name).")

    args = arg_parser.parse_args()

    cpg = CociprocessGlob()

    if args.lookup_file_path:
        cpg.LOOKUP_CSV = args.lookup_file_path

    if args.output_dir:
        cpg.INDEX_DATE_CSVPATH = '%s/index/'%args.output_dir

    full_input_path = "%s/"%args.input_dir


    cpg.init_dirs_skeleton()
    cpg.lookup_code = 0
    cpg.lookup_dic = {}
    cpg.date_dic = {}
    cpg.init_lookup_dic()


    for subdir, dirs, files in os.walk(full_input_path):
        for file in files:
            if file.lower().endswith('.json'):
                print(file)
                data = json.load(open(os.path.join(subdir, file)))
                cpg.genGlobs(data)

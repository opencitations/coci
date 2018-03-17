
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

class Cocirefprocess:

    def __init__(self):

        self.conf = {
            "email": "ivan.heibi@opencitations.net",
            "key": None,
            "agent": "COCI Maker",
            "useragent": "COCI / COCI Maker (via OpenCitations - http://opencitations.net; mailto:ivan.heibi@opencitations.net)",
            "postfix": "00000"
        }


        self.CROSSREF_CODE = '020'

        self.OUT_DATA_PATH = "data/"
        self.OUT_PROV_PATH = "prov/"
        self.LOOKUP_CSV = 'lookup.csv'
        # self.INDEX_DATE_CSVPATH = 'date.csv'
        self.INDEX_DATE_GLOBAL_CSVPATH = 'date.csv'

        self.INDEX_PROCESSED_CSVPATH = 'index/'
        self.INDEX_ERRORS_CSVPATH = 'index/'
        self.INDEX_NODOI_CSVPATH = 'index/'
        self.INDEX_FILE_CSVPATH = 'index/'
        self.INDEX_DATE_CSVPATH = 'index/'

        self.INPUT_DATA_PATH = ['crossrefdump/']

        self.MAX_DATA_ENTRIES = 10000
        self.datacsv_counter = 0
        self.file_id = 0

        self.NUMBER_ITERATIONS = 2
        self.REQUEST_TIMEOUT = 120
        self.REQ_SLEEP_TIME = 60
        self.MIN_SCORE = 75
        self.crossref_api = {
            'free_text' : 'https://api.crossref.org/works?rows=1&query=%s',
            'doi' : 'https://api.crossref.org/works/%s'
        }

        self.lookup_code = 0

        #dictionaries
        self.lookup_dic = {}
        self.processed_dic = {}
        self.date_dic = {}
        self.file_dic = {}

    ############### Methods to write on CSV files

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

    def check_make_dirs(self,filename) :
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

    def init_dirs_skeleton(self):
        self.check_make_dirs(self.OUT_DATA_PATH)
        self.check_make_dirs(self.OUT_PROV_PATH)
        self.check_make_dirs(self.INDEX_PROCESSED_CSVPATH)
        self.check_make_dirs(self.INDEX_ERRORS_CSVPATH)
        self.check_make_dirs(self.INDEX_DATE_CSVPATH)
        self.check_make_dirs(self.INDEX_NODOI_CSVPATH)
        self.check_make_dirs(self.INDEX_FILE_CSVPATH)

        self.init_csv("%sd-%s.csv"%(self.OUT_DATA_PATH,str(0)),'oci,citing,cited,creation,timespan')
        self.init_csv("%sp-%s.csv"%(self.OUT_PROV_PATH,str(0)),'oci,agent,source,datetime')

        self.INDEX_PROCESSED_CSVPATH = "%sprocessed.csv"%(self.INDEX_PROCESSED_CSVPATH)
        self.init_csv(self.INDEX_PROCESSED_CSVPATH,'id')

        self.INDEX_ERRORS_CSVPATH = "%serror.csv"%(self.INDEX_ERRORS_CSVPATH)
        self.init_csv(self.INDEX_ERRORS_CSVPATH,'id,type')

        self.INDEX_NODOI_CSVPATH = "%snodoi.csv"%(self.INDEX_NODOI_CSVPATH)
        self.init_csv(self.INDEX_NODOI_CSVPATH,'citing,cited,text')

        self.INDEX_FILE_CSVPATH = "%sfile.csv"%(self.INDEX_FILE_CSVPATH)
        self.init_csv(self.INDEX_FILE_CSVPATH,'id')

        self.INDEX_DATE_CSVPATH = "%sdate.csv" % (self.INDEX_DATE_CSVPATH)
        self.init_csv(self.INDEX_DATE_CSVPATH, 'id,value')

    def init_output_paths(self,out_o):
        self.OUT_DATA_PATH = "%s/data/"%out_o
        self.OUT_PROV_PATH = "%s/prov/"%out_o
        self.INDEX_PROCESSED_CSVPATH = '%s/index/'%out_o
        self.INDEX_ERRORS_CSVPATH = '%s/index/'%out_o
        self.INDEX_NODOI_CSVPATH = '%s/index/'%out_o
        self.INDEX_FILE_CSVPATH = '%s/index/'%out_o
        self.INDEX_DATE_CSVPATH = '%s/index/' % out_o

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
        #get lookup values again first
        self.init_lookup_dic()
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

    def init_date_dic(self):
        with open(self.INDEX_DATE_CSVPATH,'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                self.date_dic[row['id']] = row['value']

    def update_processed(self,doi_key):
        if doi_key not in self.processed_dic:
            self.processed_dic[doi_key] = 1
            self.write_txtblock_on_csv(self.INDEX_PROCESSED_CSVPATH, '\n"%s"'%(self.escape_inner_quotes(doi_key)))

    def init_processed_dic(self):
        with open(self.INDEX_PROCESSED_CSVPATH,'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                self.processed_dic[row['id']] = 1

    def update_nodoi(self,citing, cited, text):
        self.write_txtblock_on_csv(self.INDEX_NODOI_CSVPATH, '\n"%s","%s","%s"'%(self.escape_inner_quotes(citing),self.escape_inner_quotes(cited),self.escape_inner_quotes(text)))


    def init_file_dic(self):
        with open(self.INDEX_FILE_CSVPATH,'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                self.file_dic[row['id']] = 1

    def update_file_index(self,fileid):
        if fileid not in self.file_dic:
            self.file_dic[fileid] = 1
            self.write_txtblock_on_csv(self.INDEX_FILE_CSVPATH, "\n"+fileid)

    #populate all the dictionaries data
    def reload(self):

        self.init_dirs_skeleton()

        #reset dictionaries
        self.lookup_code = 0
        self.lookup_dic = {}
        self.processed_dic = {}
        self.date_dic = {}
        self.file_dic = {}

        self.init_lookup_dic()
        self.init_file_dic()
        self.init_processed_dic()
        self.init_date_dic()

        maxid = 0
        last_file = "d-0.csv"
        for subdir, dirs, files in os.walk(self.OUT_DATA_PATH):
            for file in files:
                if file.lower().endswith('.csv'):
                    matchObj = re.match( r'd-(.*).csv', file.lower() , re.M|re.I)
                    cur_id = int(matchObj.group(1))
                    if (cur_id > maxid):
                        maxid = cur_id
                        last_file = file.lower()

        dic_items = {}
        with open("%s%s"%(self.OUT_DATA_PATH,last_file), 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                dic_items[row['citing']] = 1

        self.datacsv_counter = len(dic_items)
        self.file_id = maxid

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

    def reverse_ci_to_doi(self,str_val):
        str_val = str_val[3:]
        str_doi=""
        i=0
        while i < len(str_val):
            code = str_val[i:i+2]

            for key in self.lookup_dic:
                if self.lookup_dic[key] == code:
                        str_doi = str_doi + key
            i += 2
        return "10."+str_doi

    #build a bib citation with all the available info inside the reference object
    def build_bibc(self,obj):

        unstructured = ""
        if 'unstructured' in obj:
            unstructured = obj['unstructured']

        #all att values are already in string format
        bibc = ""
        strspan= " "
        fields = [
            'issue'
            ,'first-page'
            ,'volume'
            ,'edition'
            ,'component'
            ,'standard-designator'
            ,'standards-body'
            ,'author'
            ,'year'
            ,'journal-title'
            ,'article-title'
            ,'series-title'
            ,'volume-title'
            ,'ISSN'
            ,'issn-type'
            ,'ISBN'
            ,'isbn-type'
        ]

        for f in fields:
            if f in obj:
                if obj[f] not in unstructured:
                    val_str = obj[f]
                    if f == 'ISBN':
                        val_str = val_str.replace('http://id.crossref.org/isbn/','')
                    #if f == 'ISSN':
                    #    obj[f] = obj[f].replace('http://id.crossref.org/issn/','')
                    bibc = bibc + val_str + strspan

        return bibc+unstructured


    #call crossref with the corresponding crossref_api[query_type] and the query_text
    def get_data(self,query_text, is_json = True, query_type = "free_text", num_iterations= 1, sleep_time= 60,req_timeout= None):
        api_url = self.crossref_api[query_type]
        errors = ""
        for i in range(0,num_iterations):
            api_call = api_url % (urllib.parse.quote_plus(query_text))
            #print(api_call)
            try:
                response = requests.get(api_call, headers={"User-Agent": self.conf["useragent"]}, timeout= req_timeout)
                if (response.status_code == 200):
                    if is_json:
                        return json.loads(response.text)
                    else:
                        return response.text
                else:
                    errors = errors + "HTTP error on data retrieving (HTTP status code: %s). " % str(response.status_code)
            except Exception as e:
                errors = errors + "Exception: %s " % e

            #try again after a sleep_time period
            sleep(sleep_time)

        #if the method arrives here, we got some errors
        return {"errors": errors}

    #generate the publication-date of a given crossref work object
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

    def process_list_items(self,obj, obj_file_id):
        if obj_file_id not in self.file_dic:
            list_of_items = obj['message']['items']
            for item in list_of_items:
                ##process the item
                csvdata = self.process_item(item)

                #if this is the first time i am processing this element
                if csvdata != -1:
                    if "errors" in csvdata:
                        #write the errors
                        self.write_txtblock_on_csv(self.INDEX_ERRORS_CSVPATH, '\n"%s","%s"'%(self.escape_inner_quotes(csvdata["citing_doi"]),csvdata['errors']))

                    else:

                        if (self.datacsv_counter // self.MAX_DATA_ENTRIES == 1):
                            self.datacsv_counter = 0
                            self.file_id += 1
                            self.init_csv("%sd-%s.csv"%(self.OUT_DATA_PATH,str(self.file_id)),'oci,citing,cited,creation,timespan')
                            self.init_csv("%sp-%s.csv"%(self.OUT_PROV_PATH,str(self.file_id)),'oci,agent,source,datetime')

                        if csvdata["data"] != "":
                            self.write_txtblock_on_csv("%sd-%s.csv"%(self.OUT_DATA_PATH,str(self.file_id)), csvdata["data"])
                        if csvdata["prov"] != "":
                            self.write_txtblock_on_csv("%sp-%s.csv"%(self.OUT_PROV_PATH,str(self.file_id)), csvdata["prov"])

                        #update files identifiers
                        self.datacsv_counter += 1

                    self.update_processed(csvdata["citing_doi"])

            self.update_file_index(str(obj_file_id))

    #given a crossref object get all the COCI data needed, returns an object with errors in case something wrong happend
    #returns -1 in case the object has already been processed
    def process_item(self,obj):
        data_lis = []
        prov_lis = []

        if (("DOI" in obj) and ("reference" in obj)):
            print("Processing:"+obj["DOI"])
            citing_doi = obj["DOI"].lower()
            citing_ci = self.convert_doi_to_ci(citing_doi)
            citing_date = self.build_pubdate(obj,citing_doi)

            #update dates
            #self.update_date(citing_date, citing_doi)

            #in case this is the first time i am elaborating this item
            if citing_doi not in self.processed_dic:

                data_txtblock = ""
                prov_txtblock = ""

                #iterate through all references
                for ref_item in obj['reference']:

                    ref_entry_attr = self.process_ref_entry(ref_item)
                    #in case a No-DOI request has been called
                    nodoi_text = ref_entry_attr["nodoi_text"]
                    if (nodoi_text != -1):
                        citednodoi = ""
                        if ref_entry_attr["value"] != -1:
                            citednodoi = ref_entry_attr["value"]["cited_doi"]
                        self.update_nodoi(citing_doi, citednodoi, nodoi_text)

                    ref_entry_attr = ref_entry_attr['value']
                    if(ref_entry_attr != -1):
                        if("errors" not in ref_entry_attr):

                            #create all other data needed
                            oci = citing_ci+"-"+ref_entry_attr['cited_ci']

                            timespan = ""
                            if citing_date != "" and ref_entry_attr['cited_date'] != "":

                                #citing_dt = datetime.datetime.strptime(citing_date["str_val"], citing_date["format"])
                                #cited_dt = datetime.datetime.strptime(ref_entry_attr['cited_date']["str_val"], ref_entry_attr['cited_date']["format"])
                                default_date = datetime.datetime(1970, 1, 1, 0, 0)
                                try:
                                    citing_dt = parse(citing_date, default=default_date)
                                    cited_dt = parse(ref_entry_attr['cited_date'], default=default_date)

                                    #timespan = to_iso8601(citing_dt - cited_dt)
                                    delta = relativedelta(citing_dt, cited_dt)
                                    timespan = citation.Citation.get_duration(delta,
                                                                      citation.Citation.contains_months(citing_date) and citation.Citation.contains_months(ref_entry_attr['cited_date']),
                                                                      citation.Citation.contains_days(citing_date) and citation.Citation.contains_days(ref_entry_attr['cited_date']))

                                    #in case the timespan is negative check the timespan with the year value
                                    if timespan[0] == "-" :
                                        if ref_entry_attr['cited_year'] != "":
                                            cited_year_dt = parse(ref_entry_attr['cited_year'], default=default_date)
                                            year_timespan = citation.Citation.get_duration(relativedelta(citing_dt, cited_year_dt),
                                                                              citation.Citation.contains_months(citing_date) and citation.Citation.contains_months(ref_entry_attr['cited_year']),
                                                                              citation.Citation.contains_days(citing_date) and citation.Citation.contains_days(ref_entry_attr['cited_year']))
                                            if year_timespan[0] != "-":
                                                timespan = year_timespan
                                except:
                                    pass

                            if timespan != "":
                                if timespan[0] == "-" and nodoi_text != -1:
                                    continue

                            data_txtblock = data_txtblock +'\n%s,"%s","%s",%s,%s'%(oci,self.escape_inner_quotes(citing_doi),self.escape_inner_quotes(ref_entry_attr['cited_doi']),citing_date,timespan)
                            timenow = datetime.datetime.now().replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
                            prov_txtblock = prov_txtblock + '\n%s,%s,"%s",%s'%(oci,self.conf["agent"],"https://api.crossref.org/works/%s"%(self.escape_inner_quotes(citing_doi)),timenow)

                        #we have errors
                        else:
                            #break all and return the errors
                            return {"errors": ref_entry_attr["errors"], "citing_doi": citing_doi}
                            break

                return {
                    "citing_doi": citing_doi,
                    "data": data_txtblock,
                    "prov": prov_txtblock
                }
            return - 1
        return {"errors": "entry without a DOI or Ref-List"}

    #given a reference entry returns it's DOI, CI, and Publication-Date
    #in case one of these attributes is not present: the methods returns -1
    def process_ref_entry(self,obj):

        nodoi_text = -1

        #save the year and keep it in case i don't have a date
        my_year = ""
        if "year" in obj:
            my_year = obj['year']
            if isinstance(my_year, str):
                intpart = re.search(r'\d+', my_year)
                if intpart != None:
                    if int(intpart.group()) > 0:
                        my_year = intpart.group()
                else:
                    my_year = ""
            else:
                my_year = ""

        #check if obj have a DOI if not call crossref
        if "DOI" not in obj :
            query_text = self.build_bibc(obj)
            #dont look for it
            nodoi_text = query_text
            return {'value': -1, 'nodoi_text': nodoi_text}
        else:
            cited_doi = obj["DOI"].lower()
            cited_ci = self.convert_doi_to_ci(cited_doi)
            cited_date = self.build_pubdate(obj,cited_doi)

            #in case i don't have a date, try look at it again
            if cited_date == "" :
                obj = self.get_data(cited_doi, query_type = "doi", num_iterations=self.NUMBER_ITERATIONS, sleep_time=self.REQ_SLEEP_TIME, req_timeout=self.REQUEST_TIMEOUT)
                if "errors" not in obj:
                    cited_date = self.build_pubdate(obj['message'],cited_doi)
                    if cited_date == "":
                        cited_date = my_year

            self.update_date(cited_date, cited_doi)

            return {'value': {'cited_doi': cited_doi, 'cited_ci': cited_ci, 'cited_date':cited_date,'cited_year':my_year},'nodoi_text':nodoi_text}


    def escape_inner_quotes(self,str_val):
        return str_val.replace('"', '""')


if __name__ == "__main__":
    arg_parser = ArgumentParser("cociprocess_refs_no_share.py", description="Process a crossref JSON files.")

    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The directory containing other dir with data dump.")
    arg_parser.add_argument("-glob", dest="glob_dir", required=True, help="The global index file")
    arg_parser.add_argument("-out", "--output_dir", dest="output_dir", required=False, help="The directory where the output and processing results are stored.")
    arg_parser.add_argument("-iterations", dest="num_ite", required=False, help="Maximum number of GET requests.")
    arg_parser.add_argument("-timeout", dest="timeout", required=False, help="Number of seconds before declaring a GET in timeout.")
    arg_parser.add_argument("-sleep", dest="sleep_time", required=False, help="Seconds of sleeping time between a GET request and a second try.")
    arg_parser.add_argument("-data", dest="file_entries", required=False, help="The number of entries for each csv generated.")
    arg_parser.add_argument("-n", dest="process_number", required=True, type=int,
                            help="The number of the process.")
    arg_parser.add_argument("-ds", dest="dir_step", required=True, type=int,
                            help="The step for calculating dir")
    arg_parser.add_argument("-dn", dest="dir_number", required=True, type=int,
                            help="The number of dir assigned")

    args = arg_parser.parse_args()

    cp = Cocirefprocess()

    if args.glob_dir:
        cp.LOOKUP_CSV = "%s/lookup.csv"%(args.glob_dir)
        cp.INDEX_DATE_GLOBAL_CSVPATH = "%s/date.csv"%(args.glob_dir)

    if args.file_entries:
        cp.MAX_DATA_ENTRIES = int(args.file_entries)

    if args.num_ite:
        cp.NUMBER_ITERATIONS = int(args.num_ite)

    if args.timeout:
        cp.REQUEST_TIMEOUT = int(args.timeout)

    if args.sleep_time:
        cp.REQ_SLEEP_TIME = int(args.sleep_time)

    if args.input_dir:
        cp.INPUT_DATA_PATH = []
        ending_number = args.process_number * args.dir_number
        starting_number = ending_number - (args.dir_number - 1)
        for number in range(starting_number * args.dir_step, (ending_number+1) * args.dir_step, args.dir_step):
            cp.INPUT_DATA_PATH += ["%s/%s/"%(args.input_dir, str(number))]

    print(cp.INPUT_DATA_PATH)
    exit(0)

    if args.output_dir:
        cp.init_output_paths(args.output_dir)


    cp.reload()

    print("Processing started "+str(datetime.datetime.now().replace(microsecond=0)))
    print("The input data: "+cp.INPUT_DATA_PATH)
    #iterate all the input data and process the json files
    for cur_dir in cp.INPUT_DATA_PATH:
        if os.path.exists(cur_dir):
            for subdir, dirs, files in os.walk(cur_dir):
                for file in files:
                    if file.lower().endswith('.json'):
                        print(file)
                        data = json.load(open(os.path.join(subdir, file)))
                        matchObj = re.match( r'(.*).json', file.lower() , re.M|re.I)
                        cur_id = int(matchObj.group(1))
                        cp.process_list_items(data,cur_id)

    print("Processing finished "+str(datetime.datetime.now().replace(microsecond=0)))

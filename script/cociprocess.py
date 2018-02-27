
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

conf = {
    "email": "ivan.heibi2@unibo.it",
    "key": None,
    "useragent": "cociprocess",
    "postfix": "00000"
}


CROSSREF_CODE = '020'

OUT_DATA_PATH = "data/"
OUT_PROV_PATH = "prov/"
LOOKUP_CSV = 'lookup.csv'
INDEX_PROCESSED_CSVPATH = 'index/'
INDEX_ERRORS_CSVPATH = 'index/'
INDEX_DATE_CSVPATH = 'index/'
INDEX_NODOI_CSVPATH = 'index/'
INDEX_FILE_CSVPATH = 'index/'

INPUT_DATA_PATH = 'crossrefdump/'

MAX_DATA_ENTRIES = 5
datacsv_counter = 0
file_id = 0

NUMBER_ITERATIONS = 1
REQUEST_TIMEOUT = 60
REQ_SLEEP_TIME = 10
MIN_SCORE = 75
crossref_api = {
    'free_text' : 'https://api.crossref.org/works?rows=1&query=%s&mailto='+conf["email"],
    'doi' : 'https://api.crossref.org/works/&mailto='+conf["email"]+'&query=%s'
}

lookup_code = 0

#dictionaries
lookup_dic = {}
processed_dic = {}
date_dic = {}
file_dic = {}

############### Methods to write on CSV files

#write on a csv_path file a given rows (a list of values)
#def write_row_on_csv(csv_path, new_row, csvid = None, quoting_val = csv.QUOTE_NONE):
#    if csvid != None:
#        csv_path = csv_path%(csvid)
#    with open(csv_path, 'a', newline='') as csvfile:
#        csvwriter = csv.writer(csvfile, quoting= quoting_val)
#        csvwriter.writerow(new_row)

#create new file with header
def init_csv(csv_path,header):
    check_make_dirs(csv_path)
    if not os.path.isfile(csv_path):
        with open(csv_path, 'w') as csvfile:
            csvfile.write(header)

#write on a csv_path file a given block_txt
def write_txtblock_on_csv(csv_path, block_txt):
    check_make_dirs(csv_path)
    with open(csv_path, 'a', newline='') as csvfile:
        csvfile.write(block_txt)

#write on a csv_path file a given rows (a list of values)
def write_rows_on_csv(csv_path, row_lis, csvid = None, quoting_flag= True):
    check_make_dirs(csv_path)
    block_txt = ""
    for row in row_lis:
        row_txt = ""
        separator = ","
        for field_i in range(0,len(row)):
            if (field_i == len(row) - 1):
                separator = ""
            field = row[field_i]
            if quoting_flag:
                field = '"'+field+'"'
            row_txt = row_txt + field + separator
        block_txt = block_txt + row_txt + "\n"

    if csvid != None:
        csv_path = csv_path%(csvid)
    with open(csv_path, 'a', newline='') as csvfile:
        csvfile.write(block_txt)

def check_make_dirs(filename) :
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

def init_dirs_skeleton():
    global INDEX_PROCESSED_CSVPATH,INDEX_ERRORS_CSVPATH,INDEX_DATE_CSVPATH,INDEX_NODOI_CSVPATH,INDEX_FILE_CSVPATH

    check_make_dirs(OUT_DATA_PATH)
    check_make_dirs(OUT_PROV_PATH)
    check_make_dirs(INDEX_PROCESSED_CSVPATH)
    check_make_dirs(INDEX_ERRORS_CSVPATH)
    check_make_dirs(INDEX_DATE_CSVPATH)
    check_make_dirs(INDEX_NODOI_CSVPATH)
    check_make_dirs(INDEX_FILE_CSVPATH)

    init_csv("%sd-%s.csv"%(OUT_DATA_PATH,str(0)),'oci,citing,cited,creation,timestamp')
    init_csv("%sp-%s.csv"%(OUT_PROV_PATH,str(0)),'oci,agent,source,datetime')

    INDEX_PROCESSED_CSVPATH = "%sprocessed.csv"%(INDEX_PROCESSED_CSVPATH)
    init_csv(INDEX_PROCESSED_CSVPATH,'id')

    INDEX_ERRORS_CSVPATH = "%serror.csv"%(INDEX_ERRORS_CSVPATH)
    init_csv(INDEX_ERRORS_CSVPATH,'id,type')

    INDEX_DATE_CSVPATH = "%sdate.csv"%(INDEX_DATE_CSVPATH)
    init_csv(INDEX_DATE_CSVPATH,'id,value,format')

    INDEX_NODOI_CSVPATH = "%snodoi.csv"%(INDEX_NODOI_CSVPATH)
    init_csv(INDEX_NODOI_CSVPATH,'citing,cited,text')

    INDEX_FILE_CSVPATH = "%sfile.csv"%(INDEX_FILE_CSVPATH)
    init_csv(INDEX_FILE_CSVPATH,'id')

def init_lookup_path(val):
    global LOOKUP_CSV
    LOOKUP_CSV = val

def init_file_entries(val):
    global MAX_DATA_ENTRIES
    MAX_DATA_ENTRIES = int(val)

def init_num_ite(val):
    global NUMBER_ITERATIONS
    NUMBER_ITERATIONS = int(val)

def init_timeout(val):
    global REQUEST_TIMEOUT
    REQUEST_TIMEOUT = int(val)

def init_sleep_time(val):
    global REQ_SLEEP_TIME
    REQ_SLEEP_TIME = int(val)

def init_input_paths(input_o):
    global INPUT_DATA_PATH
    INPUT_DATA_PATH = "%s/"%input_o

def init_output_paths(out_o):
    global OUT_DATA_PATH
    global OUT_PROV_PATH
    global INDEX_PROCESSED_CSVPATH
    global INDEX_ERRORS_CSVPATH
    global INDEX_DATE_CSVPATH
    global INDEX_NODOI_CSVPATH
    global INDEX_FILE_CSVPATH

    OUT_DATA_PATH = "%s/data/"%out_o
    OUT_PROV_PATH = "%s/prov/"%out_o
    INDEX_PROCESSED_CSVPATH = '%s/index/'%out_o
    INDEX_ERRORS_CSVPATH = '%s/index/'%out_o
    INDEX_DATE_CSVPATH = '%s/index/'%out_o
    INDEX_NODOI_CSVPATH = '%s/index/'%out_o
    INDEX_FILE_CSVPATH = '%s/index/'%out_o

#init the lookup_dic by the contents of its corresponding csv
def init_lookup_dic():
    with open(LOOKUP_CSV,'r') as lookupcsv:
        lookupcsv_reader = csv.DictReader(lookupcsv)
        for row in lookupcsv_reader:
            lookup_dic[row['c']] = row['code']
        #last code used
        global lookup_code
        lookup_code = len(lookup_dic) - 1

#update lookup dictionary and update its corresponding csv
def update_lookup(c):
    #define the code following the 9 rule ...
    calc_next_lookup_code()
    code = lookup_code
    global lookup_dic
    if c not in lookup_dic:
        lookup_dic[c] = code
        write_txtblock_on_csv(LOOKUP_CSV, '\n"%s","%s"'%(c,code))

def update_date(dateObj, ci_key):
    global date_dic
    if ci_key not in date_dic:
        date_dic[ci_key] = dateObj
        write_txtblock_on_csv(INDEX_DATE_CSVPATH, "\n"+ci_key+","+dateObj["str_val"]+","+str(dateObj["format"]))

def init_date_dic():
    with open(INDEX_DATE_CSVPATH,'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        global date_dic
        for row in csv_reader:
            date_dic[row['id']] = {"str_val": row['value'],"format": row['format']}

def update_processed(ci_key):
    global processed_dic
    if ci_key not in processed_dic:
        processed_dic[ci_key] = 1
        write_txtblock_on_csv(INDEX_PROCESSED_CSVPATH, "\n"+ci_key)

def init_processed_dic():
    with open(INDEX_PROCESSED_CSVPATH,'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        global processed_dic
        for row in csv_reader:
            processed_dic[row['id']] = 1

def update_nodoi(citing, cited, text):
    write_txtblock_on_csv(INDEX_NODOI_CSVPATH, '\n%s,%s,"%s"'%(citing,cited,text))


def init_file_dic():
    with open(INDEX_FILE_CSVPATH,'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        global file_dic
        for row in csv_reader:
            file_dic[row['id']] = 1

def update_file_index(file_id):
    global processed_dic
    if file_id not in file_dic:
        file_dic[file_id] = 1
        write_txtblock_on_csv(INDEX_FILE_CSVPATH, "\n"+file_id)

#populate all the dictionaries data
def reload():

    init_dirs_skeleton()

    #reset dictionaries
    global lookup_code,lookup_dic,processed_dic,date_dic,file_dic
    lookup_code = 0
    lookup_dic = {}
    processed_dic = {}
    date_dic = {}
    file_dic = {}

    init_lookup_dic()
    init_file_dic()
    init_processed_dic()
    init_date_dic()

    global datacsv_counter
    global file_id


    maxid = 0
    last_file = "d-0.csv"
    for subdir, dirs, files in os.walk(OUT_DATA_PATH):
        for file in files:
            if file.lower().endswith('.csv'):
                matchObj = re.match( r'd-(.*).csv', file.lower() , re.M|re.I)
                cur_id = int(matchObj.group(1))
                if (cur_id > maxid):
                    maxid = cur_id
                    last_file = file.lower()

    dic_items = {}
    with open("%s%s"%(OUT_DATA_PATH,last_file), 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            dic_items[row['citing']] = 1


    datacsv_counter = len(dic_items)
    file_id = maxid

###############  Convert CrossRef DOI to CI
def calc_next_lookup_code():
    global lookup_code
    rem = lookup_code % 100
    newcode = lookup_code + 1
    if (rem==89):
        newcode = newcode * 10
    lookup_code = newcode

#convert a crossref doi into a citation identifier
def convert_doi_to_ci(doi_str):
    return CROSSREF_CODE + match_str_to_lookup(doi_str)

#convert a giving string in its corresponding ci format
#using the lookup file
def match_str_to_lookup(str_val):
    ci_str = ""
    str_noprefix = str_val[3:]
    for c in str_noprefix:
        if c not in lookup_dic:
            update_lookup(c)
        ci_str = ci_str + str(lookup_dic[c])
    return ci_str

def reverse_ci_to_doi(str_val):
    str_val = str_val[3:]
    str_doi=""
    i=0
    while i < len(str_val):
        code = str_val[i:i+2]

        for key in lookup_dic:
            if lookup_dic[key] == code:
                    str_doi = str_doi + key
        i += 2
    return "10."+str_doi

#build a bib citation with all the available info inside the reference object
def build_bibc(obj):

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
def get_data(query_text, is_json = True, query_type = "free_text", num_iterations= 1, sleep_time= 60,req_timeout= None):
    api_url = crossref_api[query_type]
    errors = ""
    for i in range(0,num_iterations):
        api_call = api_url % (urllib.parse.quote_plus(query_text))
        #print(api_call)
        try:
            response = requests.get(api_call, headers={"User-Agent": conf["useragent"]}, timeout= req_timeout)
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
def build_pubdate(obj, ci):

    if ci in date_dic:
        return date_dic[ci]

    if 'issued' in obj:
        if 'date-parts' in obj['issued']:
            #is an array of parts of dates
            try:
                obj_date = obj['issued']['date-parts'][0]

                #lisdate[year,month,day]
                listdate = [1,1,1]
                for i in range(0,len(obj_date)):
                    try:
                        intvalue = int(obj_date[i])
                        listdate[i] = intvalue
                    except:
                        pass

                #I have a date , so generate it
                if (listdate[0] != 1):
                    date_val = datetime.date(listdate[0], listdate[1], listdate[2])

                    dformat = '%Y'

                    if (listdate[1] != 1):
                        dformat = dformat + '-%m'

                    if (listdate[2] != 1):
                        dformat = dformat + '-%d'


                    #e.g: 2016/3/1
                    date_in_str = date_val.strftime(dformat)

                    dateobj = {"str_val": date_in_str, "format":  dformat}
                    #date_dic[ci] = dateobj
                    return dateobj

            except IndexError:
                pass

    #date_dic[ci] = {"str_val":"","format":-1}
    return {"str_val":"","format":-1}


# given a textual input (query_txt), call crossref and retrieves the work object of
# the best scoring result in case the score is higher than MIN_SCORE
def find_work(query_txt):
    #call cross ref
    res = get_data(query_txt, num_iterations=NUMBER_ITERATIONS, sleep_time=REQ_SLEEP_TIME, req_timeout= REQUEST_TIMEOUT)

    if "errors" not in res:
        try:
            #crossref first and only result with higher score
            work_item = res['message']['items'][0]

            if "score" in work_item:
                if work_item["score"] > MIN_SCORE:
                    #check if the work has a DOI
                    if "DOI" in work_item:
                        return work_item
                    else:
                        return -1
                #low score
                return -1
        except IndexError:
                return -1
    return res

def process_list_items(obj, obj_file_id):
    if obj_file_id not in file_dic:
        list_of_items = obj['message']['items']
        for item in list_of_items:
            ##process the item
            csvdata = process_item(item)

            #if this is the first time i am processing this element
            if csvdata != -1:
                if "errors" in csvdata:
                    #write the errors
                    write_txtblock_on_csv(INDEX_ERRORS_CSVPATH, '\n%s,"%s"'%(csvdata["citing_ci"],csvdata['errors']))

                else:
                    global datacsv_counter
                    global file_id

                    if (datacsv_counter // MAX_DATA_ENTRIES == 1):
                        datacsv_counter = 0
                        file_id += 1
                        init_csv("%sd-%s.csv"%(OUT_DATA_PATH,str(file_id)),'oci,citing,cited,creation,timestamp')
                        init_csv("%sp-%s.csv"%(OUT_PROV_PATH,str(file_id)),'oci,agent,source,datetime')

                    if csvdata["data"] != "":
                        write_txtblock_on_csv("%sd-%s.csv"%(OUT_DATA_PATH,str(file_id)), csvdata["data"])
                    if csvdata["prov"] != "":
                        write_txtblock_on_csv("%sp-%s.csv"%(OUT_PROV_PATH,str(file_id)), csvdata["prov"])

                    #update files identifiers
                    datacsv_counter += 1

                update_processed(csvdata["citing_ci"])

        update_file_index(str(obj_file_id))

#given a crossref object get all the COCI data needed, returns an object with errors in case something wrong happend
#returns -1 in case the object has already been processed
def process_item(obj):
    data_lis = []
    prov_lis = []

    if (("DOI" in obj) and ("reference" in obj)):
        print("Processing:"+obj["DOI"])
        citing_doi = obj["DOI"].lower()
        citing_ci = convert_doi_to_ci(citing_doi)
        citing_date = build_pubdate(obj,citing_ci)

        #update dates
        update_date(citing_date, citing_ci)

        #in case this is the first time i am elaborating this item
        if citing_ci not in processed_dic:

            data_txtblock = ""
            prov_txtblock = ""

            #iterate through all references
            for ref_item in obj['reference']:

                ref_entry_attr = process_ref_entry(ref_item)

                if(ref_entry_attr != -1):
                    if("errors" not in ref_entry_attr):

                        #in case It was a No-DOI
                        if (ref_entry_attr["nodoi_text"] != -1):
                            update_nodoi(citing_ci, ref_entry_attr['cited_ci'], ref_entry_attr["nodoi_text"])

                        #create all other data needed
                        oci = citing_ci+"-"+ref_entry_attr['cited_ci']

                        timestamp = ""
                        if citing_date["str_val"] != "" and ref_entry_attr['cited_date']["str_val"] != "":

                            citing_dt = datetime.datetime.strptime(citing_date["str_val"], citing_date["format"])
                            cited_dt = datetime.datetime.strptime(ref_entry_attr['cited_date']["str_val"], ref_entry_attr['cited_date']["format"])

                            #timestamp = to_iso8601(citing_dt - cited_dt)
                            delta = relativedelta(citing_dt, cited_dt)
                            timestamp = citation.Citation.get_duration(delta,
                                                              citation.Citation.contains_months(citing_date["str_val"]) and citation.Citation.contains_months(ref_entry_attr['cited_date']["str_val"]),
                                                              citation.Citation.contains_days(citing_date["str_val"]) and citation.Citation.contains_days(ref_entry_attr['cited_date']["str_val"]))

                        data_txtblock = data_txtblock +"\n"+ oci+","+citing_doi+","+ref_entry_attr['cited_doi']+","+citing_date["str_val"]+","+timestamp

                        timenow = str(datetime.datetime.now().replace(microsecond=0))
                        prov_txtblock = prov_txtblock +"\n"+ oci+","+conf["useragent"]+","+crossref_api['doi']%(citing_doi)+","+timenow
                    #we have errors
                    else:
                        #break all and return the errors
                        return {"errors": ref_entry_attr["errors"], "citing_ci": citing_ci}
                        break;

            return {
                "citing_ci": citing_ci,
                "data": data_txtblock,
                "prov": prov_txtblock
            }
        return - 1
    return {"errors": "entry without a DOI or Ref-List"}

#given a reference entry returns it's DOI, CI, and Publication-Date
#in case one of these attributes is not present: the methods returns -1
def process_ref_entry(obj):

    nodoi_text = -1

    #save the year in case i don't have a date
    my_year = {"str_val":"","format":-1}
    if "year" in obj:
        try:
            date_val = datetime.date(int(obj['year']),1,1)
            date_in_str = date_val.strftime('%Y')
            my_year = {"str_val": date_in_str,"format": '%Y'}
        except:
            pass

    #check if obj have a DOI if not call crossref
    if "DOI" not in obj :
        query_text = build_bibc(obj)
        obj = find_work(query_text)
        if (obj != -1):
            nodoi_text = query_text

    if (obj != -1):
        if "errors" in obj:
            return obj
        else:
            #if my new object have a doi now
            if "DOI" in obj:
                cited_doi = obj["DOI"].lower()
                cited_ci = convert_doi_to_ci(cited_doi)

                #check if obj have a publcation date,
                #first case is true only if find_work has been called before
                cited_date = build_pubdate(obj,cited_ci)

                #in case i don't have a date, try look at it again
                if cited_date["format"] == -1 :

                    obj = get_data(cited_doi, query_type = "doi", num_iterations=NUMBER_ITERATIONS, sleep_time=REQ_SLEEP_TIME, req_timeout=REQUEST_TIMEOUT)
                    if "errors" not in obj:
                        cited_date = build_pubdate(obj['message'],cited_ci)
                        if cited_date["str_val"] == "":
                            cited_date = my_year

                #update dates
                update_date(cited_date, cited_ci)

                return {'cited_doi': cited_doi, 'cited_ci': cited_ci, 'cited_date':cited_date, 'nodoi_text':nodoi_text }
    else:
        return -1


# init_input_paths("crossrefdump")
# init_output_paths("process")
# reload()
#
# print(INPUT_DATA_PATH)
# #iterate all the input data and process the json files
# for subdir, dirs, files in os.walk(INPUT_DATA_PATH):
#     for file in files:
#         if file.lower().endswith('.json'):
#             print(file)
#             data = json.load(open(os.path.join(subdir, file)))
#             matchObj = re.match( r'(.*).json', file.lower() , re.M|re.I)
#             cur_id = int(matchObj.group(1))
#             process_list_items(data,cur_id)


if __name__ == "__main__":
    arg_parser = ArgumentParser("cociprocess.py", description="Process a crossref JSON files.")

    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The directory where of the crossref data dump.")
    arg_parser.add_argument("-out", "--output_dir", dest="output_dir", required=False, help="The directory where the output and processing results are stored.")
    arg_parser.add_argument("-iterations", dest="num_ite", required=False, help="Maximum number of GET requests.")
    arg_parser.add_argument("-timeout", dest="timeout", required=False, help="Number of seconds before declaring a GET in timeout.")
    arg_parser.add_argument("-sleep", dest="sleep_time", required=False, help="Seconds of sleeping time between a GET request and a second try.")
    arg_parser.add_argument("-data", dest="file_entries", required=False, help="The number of entries for each csv generated.")
    arg_parser.add_argument("-lookup", dest="lookup_file_path", required=False, help="The lookup file full path (with file name).")

    args = arg_parser.parse_args()

    if args.lookup_file_path:
        init_lookup_path(args.lookup_file_path)

    if args.file_entries:
        init_file_entries(args.file_entries)

    if args.num_ite:
        init_num_ite(args.num_ite)

    if args.timeout:
        init_timeout(args.timeout)

    if args.sleep_time:
        init_sleep_time(args.sleep_time)

    if args.input_dir:
        init_input_paths(args.input_dir)

    if args.output_dir:
        init_output_paths(args.output_dir)


    reload()

    print("Process starts at: "+str(datetime.datetime.now().replace(microsecond=0)))
    print("The input data: "+INPUT_DATA_PATH)
    #iterate all the input data and process the json files
    for subdir, dirs, files in os.walk(INPUT_DATA_PATH):
        for file in files:
            if file.lower().endswith('.json'):
                print(file)
                data = json.load(open(os.path.join(subdir, file)))
                matchObj = re.match( r'(.*).json', file.lower() , re.M|re.I)
                cur_id = int(matchObj.group(1))
                process_list_items(data,cur_id)

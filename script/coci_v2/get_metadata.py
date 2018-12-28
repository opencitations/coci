#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2018, Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from argparse import ArgumentParser
from csv import DictReader, writer
from io import StringIO
from os import walk, sep
from os.path import exists
from json import load
from re import sub


TYPE = "type"
ISBN = "isbn"
CONTAINER_ISBN = "container_isbn"
AUTHOR_N = "author_n"
TITLE = "title"
CITATIONS = "cited_by"
DATE = "pub_year"
THRESHOLD = 10000
BOOK_TYPES = ("monograph", "book", "edited-book", "reference-book")
BOOK_CHAPTER_TYPES = ("book-chapter", )


def normalize_doi(d_string):
    return d_string.lower().strip()


def get_doi(csv_metadata, stored_entities):
    for row in csv_metadata:
        stored_entities.add(row["doi"])


def get_citations(csv_metadata, stored_entities):
    for row in csv_metadata:
        cur_cited = normalize_doi(row["cited"])
        if cur_cited not in stored_entities:
            stored_entities[cur_cited] = 0
        stored_entities[cur_cited] += 1


def get_date(csv_metadata, stored_entities):
    for row in csv_metadata:
        cur_date = row["value"]
        if cur_date is not None:
            stored_entities[normalize_doi(row["id"])] = sub("\s+", "", cur_date).split("-")[0][:4]


def get_isbn(item, comp=None):
    result = ","

    if "ISBN" in item and item["ISBN"] is not None and (comp is None or get_type(item) in comp):
        result += "\"%s\"" % " ".join(sub("[^0-9]", "", cur_isbn) for cur_isbn in item["ISBN"])

    return result


def get_type(item):
    return item["type"] if "type" in item else ""


def get_author_number(item):
    aut_list = []

    if "author" in item and item["author"]:
        aut_list = item["author"]
    elif "editor" in item and item["editor"] and get_type(item) in BOOK_TYPES:
        aut_list = item["editor"]

    return ",\"%s\"" % str(len(aut_list))


def get_title(item):
    cur_title = ""

    if "title" in item and item["title"]:
        for title in item["title"]:
            strip_title = title.strip()
            if strip_title != "":
                if cur_title == "":
                    cur_title = strip_title
                else:
                    cur_title += " - " + strip_title

    return ",\"%s\"" % sub("\s+", " ",  cur_title.title()).strip().replace("\"", "\"\"\"")


def open_csv(f_path, func, stored_entities):
    header = None

    # Get existing data
    if f_path is not None and exists(f_path):
        with open(f_path) as f:
            csv_content = ""
            for idx, line in enumerate(f.readlines()):
                if header is None:
                    header = line
                    csv_content = header
                else:
                    if idx % THRESHOLD == 0:  # update stats
                        csv_metadata = DictReader(StringIO(csv_content), delimiter=',')
                        func(csv_metadata, stored_entities)
                        csv_content = header
                    csv_content += line

            csv_metadata = DictReader(StringIO(csv_content), delimiter=',')
            func(csv_metadata, stored_entities)
        return True

    return False


if __name__ == "__main__":
    arg_parser = ArgumentParser("get_metadata.py", description="Get the values of all the documents in Crossref.")
    arg_parser.add_argument("-i", "--in", dest="input_file", required=True,
                            help="The CSV file containing the times of interest.")
    arg_parser.add_argument("-d", "--dir", dest="input_dir", required=True,
                            help="The directory which contains the Crossref files.")
    arg_parser.add_argument("-citations", "--citations", dest="citation_file", default=None,
                            help="The CSV file containing all the citations in COCI.")
    arg_parser.add_argument("-date", "--date", dest="date_file", default=None,
                            help="The CSV file containing all the dates of DOIs.")
    arg_parser.add_argument("-type", "--type", dest="type", default=False, action="store_true",
                            help="It gets all the types from the Crossref data.")
    arg_parser.add_argument("-isbn", "--book_isbn", dest="book_isbn", default=False, action="store_true",
                            help="It gets all the ISBN from the Crossref entities of type 'book' or 'monograph'.")
    arg_parser.add_argument("-ibisbn", "--inbook_isbn", dest="inbook_isbn", default=False, action="store_true",
                            help="It gets all the ISBN of the books containing 'inbook' entities.")
    arg_parser.add_argument("-aut", "--author_number", dest="author_number", default=False, action="store_true",
                            help="It counts the number of authors of an item.")
    arg_parser.add_argument("-all", "--all_fields", dest="all_fields", default=False, action="store_true",
                            help="It considers all the fields.")


    args = arg_parser.parse_args()

    # Get existing entities
    existing_entities = set()
    input_csv_exists = open_csv(args.input_file, get_doi, existing_entities)

    # Get citation counts
    existing_citations = dict()
    open_csv(args.citation_file, get_citations, existing_citations)

    # Get DOI dates (year)
    existing_dates = dict()
    open_csv(args.date_file, get_date, existing_dates)

    header_types = []
    if args.all_fields or args.type:
        header_types.append(TYPE)
    if args.all_fields or args.book_isbn:
        header_types.append(ISBN)
    if args.all_fields or args.inbook_isbn:
        header_types.append(CONTAINER_ISBN)
    if args.all_fields or args.author_number:
        header_types.append(AUTHOR_N)
    if args.all_fields or args.title:
        header_types.append(TITLE)
    if args.citation_file:
        header_types.append(CITATIONS)
    if args.date_file:
        header_types.append(DATE)

    if header_types:
        with open(args.input_file, "a") as o:
            if not input_csv_exists:
                cur_header = "\"doi\""
                for header_type in header_types:
                    cur_header += ",\"%s\"" % header_type
                o.write("%s\n" % cur_header)

            for cur_dir, cur_subdir, cur_files in walk(args.input_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(".json"):
                        with open(cur_dir + sep + cur_file) as f:
                            cur_json = load(f)
                            if "items" in cur_json:
                                for item in cur_json["items"]:
                                    if "DOI" in item:
                                        cur_doi = normalize_doi(item["DOI"])
                                        if cur_doi not in existing_entities:
                                            existing_entities.add(cur_doi)

                                            cur_row = "\"%s\"" % cur_doi.replace("\"", "\"\"\"")
                                            for header_type in header_types:
                                                if header_type == TYPE:
                                                    cur_row += ",\"%s\"" % get_type(item)
                                                elif header_type == ISBN:
                                                    cur_row += get_isbn(item, BOOK_TYPES)
                                                elif header_type == CONTAINER_ISBN:
                                                    cur_row += get_isbn(item, BOOK_CHAPTER_TYPES)
                                                elif header_type == AUTHOR_N:
                                                    cur_row += get_author_number(item)
                                                elif header_type == TITLE:
                                                    cur_row += get_title(item)
                                                elif header_type == CITATIONS:
                                                    cit_count = existing_citations.get(cur_doi)
                                                    cur_row += ",\"%s\"" % str(cit_count if cit_count else 0)
                                                elif header_type == DATE:
                                                    year = existing_dates.get(cur_doi)
                                                    cur_row += ",\"%s\"" % (year if year else "")

                                            o.write("%s\n" % cur_row)
    else:
        print("No operation has been done, since no valid field has been specified as input.")

    print("Done.")

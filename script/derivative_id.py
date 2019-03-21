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
from urllib.parse import quote
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from unicodedata import normalize
from json import dump

BOOK_TYPES = ("monograph", "book", "edited-book", "reference-book")


def get_type(item):
    return item["type"] if "type" in item else ""


def get_date(item):
    if "issued" in item and "date-parts" in item["issued"]:
        structured_date = item["issued"]["date-parts"]
        if structured_date is not None and len(structured_date):
            return str(structured_date[0][0])[:4]


def get_author_family_count(item):
    aut_list = []

    if "author" in item and item["author"]:
        aut_list = item["author"]
    elif "editor" in item and item["editor"] and get_type(item) in BOOK_TYPES:
        aut_list = item["editor"]

    if aut_list:
        if "family" in aut_list[0]:
            family = sub("\W", "", aut_list[0]["family"])
            full_string = normalize('NFKD', family).encode("ASCII", "ignore").decode("utf-8")
            if full_string.strip() == "":
                full_string = family

            return quote(full_string.lower()), len(aut_list)


def get_norm_string(initial_string):
    # trasform the string in lowercase
    lower_string = initial_string.lower()

    # remove all the stopwords contained in the string
    nostopwords_string = " ".join([s for s in word_tokenize(lower_string) if s not in stopwords.words()])

    # remove all the characters that are not letters, spaces, or numbers
    onlywords_string = sub("[^\d\w ]", " ", nostopwords_string)

    # try to convert the string in pure ASCII form (works fine with latin characters)
    full_string = normalize('NFKD', onlywords_string).encode("ASCII", "ignore").decode("utf-8")

    # in case the conversion didn't work (e.g. for Japanese languages), it assignes directly the string obtained in the previous step
    if full_string.strip() == "":
        full_string = onlywords_string

    # it removes all the multiple spaces with a single space
    singlespace_string = sub("\s+", " ", full_string).strip()

    # it takes into consideration only the first 6 token of the string, and split them by means of a dash
    limited_dashed_string = "-".join([token for token in full_string.split()[:6]])

    # it encodes the string so as to be included in an URL
    quoted_string = quote(limited_dashed_string)

    # it returns the string
    return quoted_string

def normalize_doi(d_string):
    return d_string.lower().strip()


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

    return ",\"%s\"" % sub("\s+", " ",  cur_title.title()).strip().replace("\"", "\"\"")


if __name__ == "__main__":
    arg_parser = ArgumentParser("get_metadata.py", description="Get the values of all the documents in Crossref.")
    arg_parser.add_argument("-i", "--in", dest="input_file", required=True,
                            help="The CSV file containing the data of interest.")
    arg_parser.add_argument("-d", "--dir", dest="input_dir", required=True,
                            help="The directory which contains the Crossref files.")

    args = arg_parser.parse_args()

    res = {}
    for cur_dir, cur_subdir, cur_files in walk(args.input_dir):
        for cur_file in cur_files:
            if cur_file.endswith(".json"):
                with open(cur_dir + sep + cur_file) as f:
                    cur_json = load(f)
                    if "items" in cur_json:
                        for item in cur_json["items"]:
                            if "DOI" in item:
                                cur_doi = normalize_doi(item["DOI"])
                                author_count = get_author_family_count(item)
                                year = get_date(item)
                                title = get_title(item)
                                if author_count and year and title:
                                    derivative_id = "%s-%s-%s-%s" % (author_count[0], author_count[1], year, get_norm_string(title))
                                    if derivative_id not in res:
                                        res[derivative_id] = set()
                                    res[derivative_id].add(cur_doi)

    stat = {}
    with open(args.input_file, "w") as f:
        w = writer(f)
        for key, value in res.items():
            value_count = len(value)
            if value_count not in stat:
                stat[value_count] = 0
            stat[value_count] += 1
            w.writerow((key, "; ".join(value)))

    with open(args.input_file + ".stat", "w") as f:
        dump(stat, f, ensure_ascii=False, indent=4)

    print("Done.")

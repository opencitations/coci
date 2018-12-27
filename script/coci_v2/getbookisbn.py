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

def get_doi(csv_string):
    result = set()

    csv_metadata = DictReader(StringIO(csv_string), delimiter=',')
    for row in csv_metadata:
        result.add(row["doi"])

    return result


if __name__ == "__main__":
    arg_parser = ArgumentParser("getbookisbn.py", description="Get the ISBN of all the books in Crossref.")
    arg_parser.add_argument("-i", "--in", dest="input_file", required=True,
                            help="The CSV file containing the ISBNs.")
    arg_parser.add_argument("-d", "--dir", dest="input_dir", required=True,
                            help="The directory which contains the Crossref files.")

    args = arg_parser.parse_args()

    header = None
    threshold = 10000
    existing_types = set()
    input_csv_exists = exists(args.input_file)

    if input_csv_exists:
        with open(args.input_file) as f:
            csv_content = ""
            for idx, line in enumerate(f.readlines()):
                if header is None:
                    header = line
                    csv_content = header
                else:
                    if idx % threshold == 0:  # update stats
                        existing_types.update(get_doi(csv_content))
                        csv_content = header
                    csv_content += line

            existing_types.update(get_doi(csv_content))

    with open(args.input_file, "a") as o:
        if not input_csv_exists:
            o.write("\"doi\",\"isbn\"\n")

        for cur_dir, cur_subdir, cur_files in walk(args.input_dir):
            for cur_file in cur_files:
                if cur_file.endswith(".json"):
                    with open(cur_dir + sep + cur_file) as f:
                        cur_json = load(f)
                        if "items" in cur_json:
                            for item in cur_json["items"]:
                                if "DOI" in item:
                                    cur_doi = item["DOI"].lower().strip()
                                    if cur_doi not in existing_types:
                                        existing_types.add(cur_doi)
                                        cur_type = item["type"] if "type" in item else ""
                                        if cur_type in ("book", "monograph") and "ISBN" in item:
                                            if item["ISBN"] is not None:
                                                for cur_isbn in item["ISBN"]:
                                                    o.write("\"%s\",\"%s\"\n" % (cur_doi.replace("\"", "\"\"\""),
                                                                                 sub("[^0-9]", "", cur_isbn)))

    print("Done.")

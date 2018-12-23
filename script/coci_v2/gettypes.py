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
from json import load


def get_doi(csv_string):
    result = set()

    csv_metadata = DictReader(StringIO(csv_string), delimiter=',')
    for row in csv_metadata:
        result.add(row["doi"])

    return result


if __name__ == "__main__":
    arg_parser = ArgumentParser("gettypes.py", description="Get the types of all the documents in Crossref.")
    arg_parser.add_argument("-i", "--in", dest="input_file", required=True,
                            help="The CSV file containing the types.")
    arg_parser.add_argument("-d", "--dir", dest="input_dir", required=True,
                            help="The directory which contains the Crossref files.")

    args = arg_parser.parse_args()

    header = None
    result = {}
    threshold = 10000
    existing_types = set()

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
        if existing_types:
            o.write("\"doi\",\"type\"\n")

        for cur_files, cur_dir, cur_subdir in walk(args.input_dir):
            for cur_file in cur_files:
                if cur_file.endswith(".json"):
                    with open(cur_dir + sep + cur_file) as f:
                        cur_json = load(f)
                        if "items" in cur_json:
                            for item in cur_json["items"]:
                                if "DOI" in item:
                                    cur_type = item["type"] if "type" in item else ""
                                    o.write("\"%s\",\"%s\"\n" % (
                                        item["DOI"].lower().strip().replace("\"", "\"\"\"")), cur_type)

    print("Done.")

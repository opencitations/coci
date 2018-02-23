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

from requests import get
from argparse import ArgumentParser
from os import sep, path, makedirs, walk, remove, rmdir
from json import load
from re import sub, search
from time import sleep
from urllib.parse import quote

increment = 1000
cursor_regex = ".+\"next-cursor\":\"([^\"]+)\".+"
empty_regex = "\"items\":\[\]"
crossref_query = "https://api.crossref.org/works?filter=reference-visibility:%s,has-references:true&" \
                 "rows=%s&cursor=%s&mailto=%s"


def create_dump(args):
    conf = {
        "email": None,
        "key": None,
        "useragent": None,
        "postfix": "00000"
    }
    if path.exists(args.conf_file):
        with open(args.conf_file) as f:
            conf.update(load(f))

    headers = {"User-Agent": conf["useragent"]}
    if args.is_limited:
        visibility = "limited"
        headers["Authorization"] = "Bearer %s" % conf["key"]
    else:
        visibility = "open"

    o_dir = args.output_dir + sep + visibility
    if not path.exists(o_dir):
        makedirs(o_dir)

    cursor_file = o_dir + sep + "cursor.txt"
    if path.exists(cursor_file):
        with open(cursor_file) as f:
            cursor = f.read().strip()
    else:
        cursor = "*"

    dir_postfix = conf["postfix"]

    number_file = o_dir + sep + "number.txt"
    if path.exists(number_file):
        with open(number_file) as f:
            number = int(f.read().strip())
    else:
        number = increment

    print("Crossref dump: start the process")

    len_dir_postfix = - len(dir_postfix)
    tentative = 0
    while cursor is not None and cursor != "":
        try:
            get_url = crossref_query % (visibility, increment, quote(cursor), conf["email"])
            print("Querying Crossref API: %s" % get_url)

            r = get(get_url, headers=headers, timeout=300)
            if r.status_code == 200:
                # Find storing dir
                dir_number = 1
                str_number = str(number - 1)[:len_dir_postfix]
                if str_number != "":
                    dir_number += int(str_number)
                file_dir = o_dir + sep + str(dir_number) + dir_postfix
                if not path.exists(file_dir):
                    makedirs(file_dir)

                if search(empty_regex, r.text) is None:
                    tentative = 0
                    # Store the file
                    with open(file_dir + sep + str(number) + ".json", "w") as f:
                        f.write(r.text)
                else:
                    tentative += 1

                # Store next cursor
                if search(cursor_regex, r.text) is not None and tentative < args.stop_condition:
                    cursor = sub(cursor_regex, "\\1", r.text).replace("\/", "/")
                else:
                    cursor = ""
                with open(cursor_file, "w") as f:
                    f.write(cursor)

                # Increment number and store it
                number += increment
                with open(number_file, "w") as f:
                    f.write(str(number))
            else:
                print("Status: %s" % r.status_code)
                sleep(60)
        except Exception as e:
            print("Exception: %s" % e)
            sleep(60)

    print("Crossref dump: process finished")


def clean_dump(args):
    print("Clean Crossref dump: start")

    for cur_dir, cur_subdir, cur_files in walk(args.output_dir):
        dimension = len(cur_subdir) + len(cur_files)
        for cur_file in cur_files:
            if cur_file.endswith(".json"):
                file_path = cur_dir + sep + cur_file
                file_string = ""
                with open(file_path) as j:
                    file_string = j.read()
                if search(empty_regex, file_string) is not None:
                    remove(file_path)
                    dimension -= 1
        if dimension == 0:
            print("Remove directory '%s'" % cur_dir)
            rmdir(cur_dir)

    print("Clean Crossref dump: process finished")


if __name__ == "__main__":
    arg_parser = ArgumentParser("crossrefdump.py", description="Create a dump of the Crossref articles having "
                                                               "references in JSON.")
    arg_parser.add_argument("-o", "--out_dir", dest="output_dir", required=True,
                            help="The directory where to store the files.")
    arg_parser.add_argument("-l", "--limited", dest="is_limited", action="store_true", default=False,
                            help="To download the articles metadata with limited references.")
    arg_parser.add_argument("-c", "--conf_file", dest="conf_file", required=True,
                            help="The configuration file containing mandatory information for the request.")
    arg_parser.add_argument("-s", "--stop_condition", dest="stop_condition", default=100, type=int,
                            help="The number of maximum empty result before stopping the system.")
    arg_parser.add_argument("-cl", "--clean", dest="clean", action="store_true", default=False,
                            help="Clean the out dir (already populated with data) removing empty files and directories")

    args = arg_parser.parse_args()

    if args.clean:
        clean_dump(args)
    else:
        create_dump(args)


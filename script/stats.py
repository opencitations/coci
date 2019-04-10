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
from re import sub
from json import dump


def update(csv_string, stats, existing_ocis):
    csv_metadata = DictReader(StringIO(csv_string), delimiter=',')

    for row in csv_metadata:
        cur_oci = row["oci"]
        if cur_oci not in existing_ocis:
            existing_ocis.add(cur_oci)
            if "n_cit" not in stats:
                stats["n_cit"] = 0
            stats["n_cit"] += 1

            if "n_journal_sc" not in stats:
                stats["n_journal_sc"] = 0
            if row["journal_sc"] == "yes":
                stats["n_journal_sc"] += 1

            if "n_author_sc" not in stats:
                stats["n_author_sc"] = 0
            if row["author_sc"] == "yes":
                stats["n_author_sc"] += 1

            if "all_citing" not in stats:
                stats["all_citing"] = set()
            citing = row["citing"]
            stats["all_citing"].add(citing)

            if "all_cited" not in stats:
                stats["all_cited"] = set()
            cited = row["cited"]
            stats["all_cited"].add(cited)

            citing_prefix = sub("^([^/]+)/.*$", "\\1", citing)
            cited_prefix = sub("^([^/]+)/.*$", "\\1", cited)

            if citing_prefix not in stats:
                stats[citing_prefix] = {"citing": 0, "cited": 0}
            if cited_prefix not in stats:
                stats[cited_prefix] = {"citing": 0, "cited": 0}

            stats[citing_prefix]["citing"] += 1
            stats[cited_prefix]["cited"] += 1


if __name__ == "__main__":
    arg_parser = ArgumentParser("stats.py", description="Statistics for CSV tables of citations.")
    arg_parser.add_argument("-o", "--out", dest="output_file", required=True,
                            help="The file where to store statitistcs.")
    arg_parser.add_argument("-i", "--in", dest="input_file", required=True,
                            help="The CSV file containing citations.")

    args = arg_parser.parse_args()

    header = None
    result = {}
    threshold = 10000
    existing_ocis = set()

    with open(args.input_file) as f:
        csv_content = ""
        for idx, line in enumerate(f.readlines()):
            if header is None:
                header = line
                csv_content = header
            else:
                if idx % threshold == 0:  # update stats
                    update(csv_content, result, existing_ocis)
                    csv_content = header
                csv_content += line

        update(csv_content, result, existing_ocis)

        csv_result = [
            ("n_cit", "n_journal_sc", "n_author_sc" "n_entities", "n_citing_entities", "n_cited_entities"),
            (result["n_cit"], result["n_journal_sc"], result["n_author_sc"],
             len(result["all_citing"].union(result["all_cited"])), len(result["all_citing"]), len(result["all_cited"]))
        ]
        with open(args.output_file, "w") as g:
            csv_writer = writer(g)
            csv_writer.writerows(csv_result)

        with open(args.output_file + ".json", "w") as g:
            dump(result, g, ensure_ascii=False)


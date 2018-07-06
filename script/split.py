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
from os.path import exists
from os import makedirs, sep


def store(s, d, f_n):
    with open(d + sep + f_n) as g:
        g.write(s)


if __name__ == "__main__":
    arg_parser = ArgumentParser("crossrefdump.py", description="Create a dump of the Crossref articles having "
                                                               "references in JSON.")
    arg_parser.add_argument("-o", "--out_dir", dest="output_dir", required=True,
                            help="The directory where to store the files.")
    arg_parser.add_argument("-f", "--file", dest="file", required=True,
                            help="The file to split.")
    arg_parser.add_argument("-l", "--lines", dest="lines", required=False, default=1000000, type=int,
                            help="Number of lines per file")
    arg_parser.add_argument("-e", "--extension", dest="ext", required=False, default=".nt",
                            help="Extension of new file")

    args = arg_parser.parse_args()

    if not exists(args.out_dir):
        makedirs(args.out_dir)

    with open(args.file) as f:
        c = 0
        t = 0
        s = ""
        for line in f:
            t += 1
            if t > args.lines:
                store(s, args.out_dir, str(c) + args.ext)
                s = ""
                t = 0
                c += 1
            s += line
        if t < args.lines:
            store(s, args.out_dir, str(c) + args.ext)

    print("done")



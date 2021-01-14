#!/usr/bin/env python


"""
This script takes a file with list of directories, and produces a file with
each directory name and its size in kBytes.

If the directory does not exist, it has size 0
"""


from __future__ import print_function
import os
import argparse
import subprocess

if not hasattr(subprocess, 'check_output'):
    raise ImportError("subprocess module missing check_output(): you need python 2.7 or newer")


def get_dir_size(dirname):
    """Get size of directory using du, returned in kB"""
    cmd = 'du -s %s' % dirname
    size = int(subprocess.check_output(cmd, shell=True).split()[0])
    return size


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', help='File with list of directories, one per line')
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        raise IOError("Cannot find input file %s" % args.input)

    stem, ext = os.path.splitext(args.input)
    output_filename = stem + "_sizes" + ext
    print("Writing to", output_filename)

    total_size = 0
    with open(args.input) as inf, open(output_filename, 'w') as outf:
        for line in inf:
            size = 0
            if os.path.isdir(line.strip()):
                size = get_dir_size(line.strip())
                total_size += size
            outf.write(line.strip() + ",%d\n" % size)

    total_size_natural = total_size
    natural_unit = "kB"

    threshold = 1000

    if total_size_natural > threshold:
        total_size_natural /= threshold
        natural_unit = "MB"

    if total_size_natural > threshold:
        total_size_natural /= threshold
        natural_unit = "GB"

    if total_size_natural > threshold:
        total_size_natural /= threshold
        natural_unit = "TB"

    print("Total:", total_size_natural, natural_unit)

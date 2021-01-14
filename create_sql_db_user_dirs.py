#!/usr/bin/env python


"""Create SQLite database about all CRAB directories in user's Tier2 area.

Holds info about directory path, size, creation time, creator.
"""


from __future__ import print_function, division

import os
import argparse
import datetime
import subprocess
import create_sql_db_xml as creator


os.nice(10)


def get_user_dir_sizes(username):
    """Get Tier2 directories and their respective size

    This looks for CRAB job directories,
    e.g.  /pnfs/desy.de/cms/tier2/store/user/akaravdi/SingleElectron/crab_pickEvents/170626_204933
    ignoring any trailing 0000 etc

    # TODO check for actual ntuples. How to tell apart form non-UHH2 dir?

    Parameters
    ----------
    username : str
        Username to look for dirs. Can use more complex string to only search
        in certain directories, e.g. aggleton/RunII

    Yields
    ------
    str, float
        Directory, Size (kB)
    """
    cmd = (r"nice -n 10 find /pnfs/desy.de/cms/tier2/store/user/" + username +
           " -type d -regextype posix-egrep -regex \".*/[0-9_]{5,}\" ! -path '*/log' "
           "-exec du -sk {} \;")
    # this is python2 & 3-friendly, and ensures that each line is piped out immediately
    # this avoids a lower-level bug: https://www.turnkeylinux.org/blog/unix-buffering
    # https://stackoverflow.com/questions/2715847/read-streaming-input-from-subprocess-communicate/17698359#17698359
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, bufsize=1)
    with p.stdout:
        for line in iter(p.stdout.readline, b''):
            l = line.decode().strip()
            size, dirname = l.split()
            size = float(size.strip())
            dirname = dirname.strip()
            yield dirname, size
    p.wait()  # wait for the subprocess to exit


def get_dir_user(ntuple_dir):
    """Get "username" from filepath

    Assumes it comes after .../user/ or /group/

    e.g.:

    get_dir_user("/nfs/dust/cms/user/robin/UHH2/Ntuple_2016v2.root")
    >> robin
    get_dir_user("/pnfs/desy.de/cms/tier2//store/user/abenecke/RunII_102X_v1/PUPPIStudies/")
    >> abenecke
    get_dir_user("/pnfs/desy.de/cms/tier2//store/group/uhh/uhh2ntuples/RunII_102X_v2/"")
    >> uhh

    Parameters
    ----------
    ntuple_dir : str

    Returns
    -------
    str
        Username, or None if not found
    """
    if "/user/" not in ntuple_dir and "/group/" not in ntuple_dir:
        return None
    ntuple_dir = os.path.normpath(ntuple_dir)
    parts = ntuple_dir.split("/")
    if "/user/" in ntuple_dir:
        ind = parts.index("user")
        if ind == len(parts)-1:
            return None
        return parts[ind+1]
    elif "/group/" in ntuple_dir:
        ind = parts.index("group")
        if ind == len(parts)-1:
            return None
        return parts[ind+1]


def get_creation_time(path):
    """Get creation time of path, in ISO8601 format: YYYY-MM-DD HH:MM:SS.SSS

    This format is necessary for SQLite
    """
    stat = os.lstat(path)
    return datetime.datetime.fromtimestamp(stat.st_ctime).isoformat(' ')


def get_user_dir_data(username):
    """Get data about each CRAB directory corresponding to user `username`,
    looking in /pnfs/desy.de/cms/tier2/store/user/"+username

    Currently returns info about:
    - dirname: directory name
    - size: directory size in kB
    - user: owning user (allows `username` param to have subdirs)
    - creation time: directory creation time

    Parameters
    ----------
    username : str
        Username (can also add subdirectory patterns, e.g. "aggleton/RunII*")

    Yields
    ------
    dict
        Data in a dict
    """
    for dirname, size in get_user_dir_sizes(username):
        user = get_dir_user(dirname)
        if user is None:
            user = ""
        print(dirname)
        data = dict(
            dirname=dirname,
            size=float(size),
            user=user,
            creation_time=get_creation_time(dirname)
        )
        yield data


def create_user_dir_table(username, output_filename, table_name='user_dir', append=True):
    """Main function to create table of user's directories

    Parameters
    ----------
    username : str
        Tier2 username to look for directories
    output_filename : str
        SQL output filename
    table_name : str, optional
        Name of table in SQL file
    append : bool, optional
        If True, then append to any existing table with `table_name`.
        Otherwise, delete existing before adding entries
    """
    user_dir_table_maker = creator.SQLTable(table_name)
    user_dir_table_maker.create_connection(path=output_filename)
    if not append:
        user_dir_table_fields = [
            "dirname TEXT NOT NULL",
            "size FLOAT",
            "user TEXT",
            "creation_time TEXT"
        ]
        user_dir_table_maker.create_table(table_fields=user_dir_table_fields)

    print("Filling user dir table...")
    user_dir_table_maker.fill_table(get_user_dir_data(username))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("user",
                        help="CERN username to test")
    parser.add_argument("--output",
                        default="xml_table.sqlite",
                        help="Output SQL filename")
    parser.add_argument("--append",
                        action='store_true',
                        help="If True, append data to existing table in --output, if one exists. "
                             "Otherwise, overwrites tables contents")
    args = parser.parse_args()
    if not os.path.isfile(args.output) and args.append:
        print("Output does not exist, setting --append False")
        args.append = False

    create_user_dir_table(username=args.user,
                          output_filename=args.output,
                          append=args.append)

#!/usr/bin/env python


"""Create SQLite XML & XML ntuple directory databases.

One table holds info about XML files.
Another table holds info about all the ntuple directories inside those XML files.

Can either read from a UHH2-datasets repo (using --uhh2datasetsDir),
or can iterate over old branches of UHH2 repo, looking in common/data (using --legacy).
"""


from __future__ import print_function, division

import os
import re
import sqlite3
import argparse
import datetime
from time import sleep
import subprocess

import findAllNtupleDirs as finder


os.nice(10)


class SQLTable(object):

    def __init__(self, table_name):
        self.connection = None
        self.table_name = table_name

    def create_connection(self, path):
        self.connection = None
        try:
            self.connection = sqlite3.connect(path)
        except sqlite3.Error as e:
            print("Error", e, "occurred in create_connection")
            raise

    def execute_query(self, query, args=None):
        try:
            with self.connection:
                self.connection.execute(query, args or tuple())
        except sqlite3.Error as e:
            print("Error", e, "occurred in execute_query")
            raise

    def create_table(self, table_fields):
        self.execute_query("DROP TABLE IF EXISTS %s;" % self.table_name)
        create_table_cmd = """
            CREATE TABLE "{table_name}" ({table_field_str});
            """.format(table_name=self.table_name, table_field_str=",".join(table_fields))
        self.execute_query(create_table_cmd)

    def insert_into_table(self, data):
        """Insert single row of data into table

        Parameters
        ----------
        connection : sqlite3.Connection
            Description
        table_name : str
            Name of table
        data : dict
            The keys and corresponding values are used as the columns names and values
        """
        # Generate command, using sqlite ? placeholders, although this is only
        # for values, not parameters like table name, column names
        # So we have to generate that ourselves
        insert_cmd = """
            INSERT INTO "{table_name}" ({column_names})
            VALUES ({values_pattern});
        """.format(table_name=self.table_name,
                   column_names=", ".join(data.keys()),  # order is same for .keys() and .values()
                   values_pattern=",".join(["?"] * len(data)))
        self.execute_query(insert_cmd, tuple(data.values()))

    def fill_table(self, data_generator, verbose=True):
        counter = 0
        for data in data_generator:
            self.insert_into_table(data)
            counter += 1
        print("Added", counter, "entries to table")


class XMLFileDataGenerator(object):

    def __init__(self, top_dir, git_source):
        self.top_dir = top_dir
        self.git_source = git_source

    def get_xml_filenames(self):
        """Summary

        Yields
        ------
        TYPE
            Description
        """
        for root, dirs, files in os.walk(self.top_dir):
            for filename in files:
                if os.path.splitext(filename)[1] != ".xml":
                    continue
                full_filename = os.path.join(root, filename)
                # only want relative to UHH2-datasets or whatever top_dir is
                full_filename = os.path.relpath(full_filename, self.top_dir)
                yield full_filename

    @staticmethod
    def get_year_from_path(path):
        """Get dataset year from XML filepath.

        Assumes it comes after RunII_*, or is the first part
        e.g.
        get_year_from_dir("../common/dataset/RunII_102X_v1/2017v2/MC_TTbar.xml")
        >> "2017v2"

        Parameters
        ----------
        path : str

        Returns
        -------
        str
            Year, or None if not found
        """
        parts = path.split("/")
        branch_pattern = "RunII_"

        def _is_year(s):
            return (("16" in s
                     or "17" in s
                     or "18" in s)
                    and ".xml" not in s)

        for p in parts:
            if branch_pattern in p:
                ind = parts.index(p)
                if ind == len(parts)-1:
                    return None
                year = parts[ind+1]
                return year if _is_year(year) else None

        for p in parts:
            if _is_year(p):
                return p

        return None

    @staticmethod
    def get_branch_from_path(path):
        """Summary

        Parameters
        ----------
        path : TYPE
            Description

        Returns
        -------
        TYPE
            Description
        """
        parts = path.split("/")
        branch_pattern = "RunII_"
        # in legacy branches, might have MC_94X_v1
        branch_pattern2 = r"[0-9]+X_"
        for p in parts:
            if ((branch_pattern in p or re.search(branch_pattern2, p))
                and ".xml" not in p):
                return p
        return None

    def get_xml_data(self, xml_path):
        path = os.path.normpath(xml_path)
        year = self.get_year_from_path(path)
        if year is None:
            # print("Can't get year from %s" % path)
            year = ""
            # raise ValueError("Can't get year from %s" % path)
        branch = self.get_branch_from_path(path)
        if branch is None:
            # print("Can't get branch from %s" % path)
            # raise ValueError("Can't get branch from %s" % path)
            branch = ""
        return dict(filepath=xml_path,
                    year=year,
                    branch=branch,
                    git_src=self.git_source)

    def __iter__(self):
        """Summary

        Yields
        ------
        TYPE
            Description
        """
        for xml_path in self.get_xml_filenames():
            yield self.get_xml_data(xml_path)


class XMLNtupleDirDataGenerator(object):

    def __init__(self, top_dir):
        self.top_dir = top_dir

    def get_xml_filenames(self):
        """Summary

        Yields
        ------
        TYPE
            Description
        """
        for root, dirs, files in os.walk(self.top_dir):
            for filename in files:
                if os.path.splitext(filename)[1] != ".xml":
                    continue
                full_filename = os.path.join(root, filename)
                # only want relative to UHH2-datasets or whatever top_dir is
                full_filename = os.path.relpath(full_filename, self.top_dir)
                yield full_filename

    @staticmethod
    def get_ntuple_filenames_from_xml(xml_filename):
        """Yield ntuple filenames from XML file

        Parameters
        ----------
        xml_filename : str
            XML file to get ntuples from

        Yields
        ------
        str
            Ntuple filename
        """
        with open(xml_filename) as f:
            is_comment = False
            fname_pattern = r'< ?In FileName="(.+)" Lumi="0\.0" ?\/>'
            for line in f:
                line = line.strip()
                if line.startswith("<!--"):
                    is_comment = True
                if line.endswith("-->"):
                    is_comment = False
                    continue
                if is_comment:
                    continue

                match = re.search(fname_pattern, line.strip())
                if match is None:
                    continue
                else:
                    fname = match.group(1)
                    yield fname

    def get_ntuple_dirs_from_xml(self, xml_filename):
        dirs = set()
        for ntuple_fname in self.get_ntuple_filenames_from_xml(xml_filename):
            dir_name = os.path.normpath(os.path.dirname(ntuple_fname))
            parts = dir_name.split("/")
            # remove last 0000 directory that CRAB makes
            if re.match(r"^[0-9]{4}$", parts[-1]):
                dir_name = "/".join(parts[:-1])
            dirs.add(dir_name)
        return list(dirs)

    @staticmethod
    def get_size(path):
        """Get size of directory/file path in kB"""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    stat = os.lstat(fp)
                except OSError:
                    continue
                total_size += stat.st_size
        return total_size / 1024

    @staticmethod
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

    @staticmethod
    def get_creation_time(thing):
        """Get creation time of path, in ISO8601 format: YYYY-MM-DD HH:MM:SS.SSS

        This format is necessary for SQLite
        """
        stat = os.lstat(thing)
        return datetime.datetime.fromtimestamp(stat.st_ctime).isoformat(' ')

    def get_ntuple_dir_data(self, ntuple_dir):
        size = -1
        user = "None"
        creation_time = "-1"
        user = self.get_dir_user(ntuple_dir)  # doesn't rely on it actually existing
        if os.path.isdir(ntuple_dir):
            size = self.get_size(ntuple_dir)
            creation_time = self.get_creation_time(ntuple_dir)
        data = dict(ntuple_dir=ntuple_dir,
                    size=size,
                    user=user,
                    creation_time=creation_time)
        return data

    def __iter__(self):
        """Summary

        Yields
        ------
        TYPE
            Description
        """
        counter = 0
        for xml_path in self.get_xml_filenames():
            for ntuple_dir in self.get_ntuple_dirs_from_xml(os.path.join(self.top_dir, xml_path)):
                counter += 1
                # Sleep every so often to avoid too much stress on filesystem
                if counter % 1000 == 0:
                    print("Done", counter, ", sleeping for 5s...")
                    sleep(5)

                data = self.get_ntuple_dir_data(ntuple_dir)
                data['xml_filepath'] = xml_path
                yield data


def get_git_remote_info():
    """Get current git remote name and URL

    Returns
    -------
    str, str
        Name, URL
    """
    remote_str = subprocess.check_output("git remote -v".split())
    remote_name, remote_url, _ = remote_str.splitlines()[0].split()
    remote_name = remote_name.decode()
    remote_url = remote_url.decode()
    if remote_url.endswith(".git"):
        repo_name = remote_url.split("/")[-1].replace(".git", "")
    else:
        remote_url = os.path.normpath(remote_url)
        repo_name = remote_url.split("/")[-1]

    return remote_name, repo_name


def get_git_current_branch():
    """Get current git branch name

    Returns
    -------
    str
    """
    # this works in older git versions as well
    branch = subprocess.check_output("git rev-parse --abbrev-ref HEAD".split())
    branch = branch.decode().strip()
    return branch


def create_xml_table(top_dir, output_filename, table_name="xml", append=True):
    """Make and fill SQL table with XML file info

    Parameters
    ----------
    top_dir : str
        Top directory to start looking for XML files
    output_filename : str
        Output SQL filename
    table_name : str, optional
        Name of table in SQL file

    Raises
    ------
    IOError
        If `top_dir` doesn't exist
    """
    if not os.path.isdir(top_dir):
        raise IOError("%s does not exist" % top_dir)
    xml_table_maker = SQLTable(table_name)
    xml_table_maker.create_connection(path=output_filename)

    if not append:
        # TODO put schema somewhere more central/connected with data dict
        xml_table_fields = [
            "filepath TEXT NOT NULL",
            "branch TEXT NOT NULL",
            "git_src TEXT",  # the UHH2 git branch, or UHH2-datasets
            "year TEXT NOT NULL"
        ]
        xml_table_maker.create_table(table_fields=xml_table_fields)

    pwd = os.getcwd()
    os.chdir(top_dir)
    remote_name, remote_url = get_git_remote_info()
    branch = get_git_current_branch()
    os.chdir(pwd)

    xml_generator = XMLFileDataGenerator(top_dir=top_dir,
                                         git_source="%s/%s" % (remote_url, branch))
    print("Filling xml table...")
    xml_table_maker.fill_table(xml_generator)


def create_xml_ntuple_dir_table(top_dir, output_filename, table_name="xml_ntuple_dir", append=True):
    """Make and fill SQL table with Ntuple directory info from XML files

    Parameters
    ----------
    top_dir : str
        Top directory to start looking for XML files
    output_filename : str
        Output SQL filename
    table_name : str, optional
        Name of table in SQL file

    Raises
    ------
    IOError
        If `top_dir` doesn't exist
    """
    if not os.path.isdir(top_dir):
        raise IOError("%s does not exist" % top_dir)
    xml_ntuple_dir_table_maker = SQLTable(table_name)
    xml_ntuple_dir_table_maker.create_connection(path=output_filename)
    if not append:
        xml_ntuple_dir_table_fields = [
            "xml_filepath TEXT NOT NULL",
            "ntuple_dir TEXT NOT NULL",
            "size FLOAT",
            "user TEXT",
            "creation_time TEXT"
        ]
        xml_ntuple_dir_table_maker.create_table(table_fields=xml_ntuple_dir_table_fields)

    xml_ntuple_dir_generator = XMLNtupleDirDataGenerator(top_dir=top_dir)
    print("Filling xml ntuple dir table...")
    xml_ntuple_dir_table_maker.fill_table(xml_ntuple_dir_generator)


def create_tables(top_dir, output_filename, append):
    """Over-arching method to create all the tables"""
    create_xml_table(top_dir=top_dir, output_filename=output_filename, append=append)
    create_xml_ntuple_dir_table(top_dir=top_dir, output_filename=output_filename, append=append)


def make_tables_for_legacy_branches(output_filename, append=True):
    """Main function to make tables for XML files in UHH2/common/datasets,
    iterating through all the relevant branches. For each, we checkout the code,
    then scan over the XMLs in common/datasets.

    Parameters
    ----------
    output_filename : str
        Name of output SQL file
    append : bool, optional
        If True, and the table already exists in `output_filename`,
        then append entries. Otherwise empty table first.
    """
    # Setup UHH2 in clean directory avoid any contamination
    output_filename = os.path.abspath(output_filename)

    deploy_dirname = "UHHCounting"
    if not os.path.isdir(deploy_dirname):
        print("Cloning repo since I can't find an existing clone under", deploy_dirname)
        finder.init_repo("https://github.com/UHH2/UHH2.git", deploy_dirname)
    else:
        os.chdir(deploy_dirname)

    # Figure out which branches to look at based on what user wants,
    # and what is available
    our_list_of_branches = [finder.REMOTE_NAME+"/"+x for x in finder.LEGACY_BRANCHES]

    list_of_remote_branches = finder.get_all_remote_branches()
    # list_of_local_branches = finder.get_all_local_branches()

    important_branches = sorted(list(set(our_list_of_branches) & set(list_of_remote_branches)))
    print("Only looking in branches:", important_branches)

    if not append and os.path.isfile(output_filename):
        os.remove(output_filename)

    for ind, remote_branch in enumerate(important_branches):
        remote_branch = remote_branch.split("/")[1]
        local_branch_name = remote_branch
        finder.checkout_branch(remote_branch, local_branch_name)
        finder.pull_branch()
        this_append = ((ind == 0) and append) or (ind != 0)
        # must be True to store multiple branches in same table
        create_xml_table(top_dir="common/datasets",
                         output_filename=output_filename,
                         append=this_append)
        create_xml_ntuple_dir_table(top_dir="common/datasets",
                                    output_filename=output_filename,
                                    append=this_append)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--uhh2datasetsDir",
                             help="Top directory to look for XML files. "
                                  "All subdirectories will be included, recursively."
                                  "e.g. UHH2-datasets dir")
    input_group.add_argument("--legacy",
                             action='store_true',
                             help="If True, checks out a copy of UHH2 repo, "
                                  "iterates over branches of UHH2 repo, "
                                  "and looks for XMl files in common/datasets.")
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

    if args.uhh2datasetsDir:
        create_tables(top_dir=args.uhh2datasetsDir,
                      output_filename=args.output,
                      append=args.append)
    elif args.legacy:
        make_tables_for_legacy_branches(output_filename=args.output,
                                        append=args.append)

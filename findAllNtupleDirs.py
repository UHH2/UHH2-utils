#!/usr/bin/env python

"""
This script goes through various branches of UHH2 code and looks for NTuple
files in use taken from common/datasets/*.xml

This is to ensure we keep don't use lots of space unnecessarily.

The code checks out a fresh copy of UHH2, then checks out each branch, looks
for all ROOT filenames in the XML files, and saves a list of them to a txt file.

The user should update LEGACY_BRANCHES as appropriate.

Note that for 102X and 106X branches, it instead checks out and looks in a
copy of the UHH2-datasets repository.
"""


from __future__ import print_function
import os
import re
import sys
import subprocess
import uuid
import shutil

if not hasattr(subprocess, 'check_output'):
    raise ImportError("subprocess module missing check_output(): you need python 2.7 or newer")


# Only check these branch names in UHH2/common/datasets
LEGACY_BRANCHES = [
    # "RunII_102X_v2",  # these are now stored in uhh2-datasets repo, do separately
    # "RunII_102X_v1",
    "RunII_101_v1",
    "RunII_94X_v3",
    "RunII_94X_v2",
    "RunII_94X_v1",
    "RunII_80X_v6",
    "RunII_80X_v5",
    "RunII_80X_v4",
    "RunII_80X_v3",
]


# Set this to the remote name that will be used for the central UHH2 repo
REMOTE_NAME = "UHH"


def init_repo(repo_url, clone_dir):
    if os.path.isdir(clone_dir):
        print(clone_dir+" already exists, deleting")
        shutil.rmtree(clone_dir)
    os.makedirs(clone_dir)
    os.chdir(clone_dir)
    subprocess.check_call("git init", shell=True)
    subprocess.check_call("git remote add "+REMOTE_NAME+" "+repo_url, shell=True)
    subprocess.check_call("git fetch "+REMOTE_NAME, shell=True)
    print(os.getcwd())


def get_all_remote_branches():
    cmd = "git --no-pager branch -r"
    out = subprocess.check_output(cmd, shell=True)
    return [x.strip() for x in out.decode().splitlines()]


def get_all_local_branches():
    cmd = "git --no-pager branch"
    out = subprocess.check_output(cmd, shell=True)
    return [x.strip().strip("*").strip() for x in out.decode().splitlines()]


def checkout_branch(remote_branch_name, local_branch_name):
    cmd = "git fetch -u %s %s:%s" % (REMOTE_NAME, remote_branch_name, local_branch_name)
    subprocess.check_call(cmd, shell=True)
    print("Checking out", remote_branch_name, "to", local_branch_name)
    cmd = "git checkout %s" % (local_branch_name)
    subprocess.check_call(cmd, shell=True)
    # set tracking
    cmd = 'git branch -u %s/%s %s' % (REMOTE_NAME, remote_branch_name, local_branch_name)
    subprocess.check_call(cmd, shell=True)


def pull_branch():
    # assumes tracking branch
    cmd = 'git pull'
    subprocess.check_call(cmd, shell=True)


def find_xml_files(start='common/datasets'):
    xml_filenames = []
    for root, dirs, files in os.walk(start):
        for filename in files:
            if os.path.splitext(filename)[1] == ".xml":
                xml_filenames.append(os.path.join(root, filename))
    return xml_filenames


def get_root_files_from_xml(xml_filename):
    root_filenames = []
    with open(filename) as f:
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
                root_filenames.append(match.group(1))
    return root_filenames


def remove_crab_dir(dirname):
    """If dir path ends with e.g. 0001 added by crab, remove it"""
    dirname = dirname.rstrip("/")  # a trailing / will screw up basename
    last_dir = os.path.basename(dirname)
    if re.match(r'^\d\d\d\d$', last_dir):
        return os.path.dirname(dirname)
    else:
        return dirname


def save_list_to_file(this_list, output_filename):
    with open(output_filename, "w") as f:
        f.write("\n".join(this_list))


def do_legacy_branches(check_missing):
    """Handle the UHH2/common/datasets directories for legacy branches"""
    # Setup UHH2 in clean directory avoid any contamination
    deploy_dirname = "UHHCounting"
    if not os.path.isdir(deploy_dirname):
        print("Cloning repo since I can't find an existing clone under", deploy_dirname)
        init_repo("https://github.com/UHH2/UHH2.git", deploy_dirname)
    else:
        os.chdir(deploy_dirname)

    # Figure out which branches to look at based on what user wants,
    # and what is available
    our_list_of_branches = [REMOTE_NAME+"/"+x for x in LEGACY_BRANCHES]

    list_of_remote_branches = get_all_remote_branches()
    list_of_local_branches = get_all_local_branches()

    important_branches = sorted(list(set(our_list_of_branches) & set(list_of_remote_branches)))
    print("Only looking in branches:", important_branches)

    for remote_branch in important_branches[:]:
        all_root_files = []
        remote_branch = remote_branch.lstrip(REMOTE_NAME+"/")
        local_branch_name = remote_branch
        checkout_branch(remote_branch, local_branch_name)
        pull_branch()
        xml_files = find_xml_files()
        these_root_files_lists = [get_root_files_from_xml(x) for x in xml_files]
        for l in these_root_files_lists:
            all_root_files.extend(l)

        # Write missing files to file
        if check_missing:
            print("Doing missing files")
            missing_counter = 0
            with open("../%s_missing.txt" % remote_branch, "w") as f:
                for xf in xml_files:
                    first_time = True
                    these_root_files = get_root_files_from_xml(xf)
                    for rf in these_root_files:
                        if not os.path.isfile(rf):
                            missing_counter += 1
                            if first_time:
                                f.write(xf + "::\n")
                                first_time = False
                            f.write(rf + "\n")
            print("# Missing files:", missing_counter)

        # Write list of all filenames
        all_root_files = sorted(list(set(all_root_files)))
        file_log_filename = "ntuple_filenames_"+remote_branch+".txt"
        # use .. as we're in the UHH repo
        save_list_to_file(all_root_files, "../"+file_log_filename)
        print("Found", len(all_root_files), "ntuples, list saved to", file_log_filename)

        # Write list of all directory names
        all_root_files_dirs = sorted(list(set([remove_crab_dir(os.path.dirname(f))
                                               for f in all_root_files])))
        dir_log_filename = "ntuple_dirnames_"+remote_branch+".txt"
        # use .. as we're in the UHH repo
        save_list_to_file(all_root_files_dirs, "../"+dir_log_filename)
        print("Found", len(all_root_files_dirs), "ntuple dirs, list saved to", dir_log_filename)

        # Write map of dirname -> XMLs
        print("Doing dir map")
        these_root_dirs_lists = [sorted(list(set([remove_crab_dir(os.path.dirname(r))
                                                  for r in rfl])))
                                 for rfl in these_root_files_lists]
        with open("../%s_dir_map.txt" % remote_branch, "w") as f:
            for rd in all_root_files_dirs:
                f.write(rd + "::\n")
                for ind, rdl in enumerate(these_root_dirs_lists):
                    if rd in rdl:
                        f.write("\t" + xml_files[ind].lstrip("common/datasets/") + "\n")
    os.chdir("..")


def do_new_branches(check_missing):
    """Handle the 102X and 106X branches: these use UHH2-datasets repo"""
    # Clone UHH2-datasets repo if necessary
    datasets_dirname = 'UHH2-datasets'
    if not os.path.isdir(datasets_dirname):
        print("Cloning repo since I can't find an existing clone under", datasets_dirname)
        clone_repo = True
        init_repo('https://github.com/UHH2/UHH2-datasets', datasets_dirname)
    else:
        os.chdir(datasets_dirname)
    checkout_branch('master', 'master')
    pull_branch()

    # Instead of iterating through branches, we iterate through directories,
    # each of which corresponds to a release
    releases = [x for x in os.listdir('.') if os.path.isdir(x) and 'RunII' in x]
    print('Considering', releases, 'in UHH2-datasets')
    for release in releases:
        # Do usual finding of XML files, check missing, save to txt files
        xml_files = find_xml_files(start=release)
        these_root_files_lists = [get_root_files_from_xml(x) for x in xml_files]
        all_root_files = []
        for l in these_root_files_lists:
            all_root_files.extend(l)

        # Write missing files to file
        if check_missing:
            print("Doing missing files")
            missing_counter = 0
            # use .. as we're in the UHH repo
            with open("../%s_missing.txt" % release, "w") as f:
                for xf in xml_files:
                    first_time = True
                    these_root_files = get_root_files_from_xml(xf)
                    for rf in these_root_files:
                        if not os.path.isfile(rf):
                            missing_counter += 1
                            if first_time:
                                f.write(xf + "::\n")
                                first_time = False
                            f.write(rf + "\n")
            print("# Missing files:", missing_counter)

        # Write list of all filenames
        all_root_files = sorted(list(set(all_root_files)))
        file_log_filename = "ntuple_filenames_"+release+".txt"
        # use .. as we're in the UHH repo
        save_list_to_file(all_root_files, "../"+file_log_filename)
        print("Found", len(all_root_files), "ntuples, list saved to", file_log_filename)

        # Write list of all directory names
        all_root_files_dirs = sorted(list(set([remove_crab_dir(os.path.dirname(f))
                                               for f in all_root_files])))
        dir_log_filename = "ntuple_dirnames_"+release+".txt"
        # use .. as we're in the UHH repo
        save_list_to_file(all_root_files_dirs, "../"+dir_log_filename)
        print("Found", len(all_root_files_dirs), "ntuple dirs, list saved to", dir_log_filename)

        # Write map of dirname -> XMLs
        print("Doing dir map")
        these_root_dirs_lists = [sorted(list(set([remove_crab_dir(os.path.dirname(r))
                                                  for r in rfl])))
                                 for rfl in these_root_files_lists]
        with open("../%s_dir_map.txt" % release, "w") as f:
            for rd in all_root_files_dirs:
                f.write(rd + "::\n")
                for ind, rdl in enumerate(these_root_dirs_lists):
                    if rd in rdl:
                        f.write("\t" + xml_files[ind].lstrip("common/datasets/") + "\n")

    os.chdir("..")


def main(check_missing=True):
    t2_example_dir = '/pnfs/desy.de/cms/tier2/'
    if check_missing and not os.path.isdir(t2_example_dir):
        print("Cannot find", t2_example_dir, " - skipping missing file check")
        check_missing = False

    do_legacy_branches(check_missing)
    do_new_branches(check_missing)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkMissing',
                        help='Compile lists of ntuples in XMLs that no longer exist on disk (slow)',
                        action='store_true')
    args = parser.parse_args()
    sys.exit(main(check_missing=args.checkMissing))

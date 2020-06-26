# UHH2-utils

Collection of scripts, etc, that are useful for UHH2-related things.

e.g. collating statistics about ntuples, manipulating XMLs

## Installation

_TODO_

## Utilities


The following 4 scripts are to tackle the missing files from the broken dCache drive. They figure out the runs & lumisections in the "bad" files, and create a JSON mask for them, to be using in a CRAB config (e.g. `crab_template.py`).

### commentOutBadXML.sh

Create a copy of a XML file, commenting out ntuples listed in a plain txt file

### xmlToTxt.sh

Convert XML file of ntuples to plain text file of them, ignoring any commented-out lines

### dump_lumilist.C

ROOT script to save all the run numbers & lumisections in ntuple(s) to JSON. Can either accept a filepath (with globbing), or a text file with a list of Ntuple filenames.
The lumi JSON can then be used with the standard lumilist tools: https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideGoodLumiSectionsJSONFile

### splitAndDumpLumiList.sh

Script to run dump_lumilist over large lists of files, by dividing into jobs of 1000, and running these in parallel.
Expects file with list of files to end in .txt, can then use mergeJSON.py to combine the results.

### splitGoldenJSONbyRunPeriod.sh

Download Golden JSON for a chosen year, and split it into individual JSON files for each run period.

### lumi_list_from_das.py

Gets lumilist for dataset from DAS. Only accepts one dataset (& its ext sample, if it exists).

#### Re-processing of missing lumis

1. Create XML excluding missing ntuples:

```
./commentOutBadXML.sh <XML filename> <list of missing ntuples>
```

makes `X_nobad.xml`

2. Convert that into plain txt:

```
./xmlToTxt.sh X_nobad.xml X_nobad.txt
```

3. Create lumilist from txt file:

```
root -q -b 'dump_lumilist.C("X_nobad.txt","lumilist_X_nobad.json")'
```

Can use `splitAndDumpLumiList.sh X_nobad.txt lumilist_X_nobad.json` instead to split into parallel processes.

4. Only for **data**: if not already done, create Golden JSON per Run period:

```
./splitGoldenJSONbyRunPeriod.sh 2016
```

Then diff Golden JSON & list of "good" json, e.g.:

```
compareJSON.py --sub Golden_2016_RunA.json lumilist_X_nobad.json missing_2016_RunA.json
```

You can then use `missing_2016_RunA.json` in your `crab_template.py` in the `config.Data.lumiMask` attribute.

5. Only for **MC**: create a reference lumilist JSON for your sample:

```
./lumi_list_from_das.py <DATASET NAME> dataset_all.json
```

Then diff the reference and your json of "good" lumisections, e.g.:

```
compareJSON.py --sub dataset_all.json lumilist_X_nobad.json missing.json
```

You can then use `missing.json` in your `crab_template.py` in the `config.Data.lumiMask` attribute.

========

### cleanupXML.sh

Removes ROOT files from XML files that are marked as "missing" from running `datasetInfo.py`

### datasetInfo.py

Go through directory of XML files, and save info to CSV file, e.g. user, year, etc.

Also makes list of missing ntuple files.

### findAllNtupleDirs.py

Go through **all** relevant branches of UHH2, collate list of Ntuple directorys & filenames used in each by scanning all XML files.

Also produces map of directory -> XMLs, and list of missing files

_TODO: unify this with datasetInfo.py, lots of overlap_

_Bigger TODO: make into database for easier querying etc?_

### crabKillXMLCheck.py

Check XML against CRAB log & remove files that crab thought were still transferring.

This is necessary if you then use notFinishedLumis.json from crab report, since that will contain jobs that CRAB thought were transferring *even if* they look OK on the T2. This therefore avoids duplicate events.

### DAGstatus

Utility to pretty-print status from condor DAG jobs.

This requires you to add the following to your DAG file:

```
NODE_STATUS_FILE <status file> <time interval> ALWAYS-UPDATE
```

See: https://htcondor.readthedocs.io/en/latest/users-manual/dagman-applications.html#capturing-the-status-of-nodes-in-a-file

Usage:

```
DAGstatus [-s] <status file> [<status file> <status file>...]
```

The script handles 1 or more DAG status files.
By default, it prints info for each job in a DAG.
Using the `-s` option only produces a one-line summary for each DAG.

The colours and formatting are configurable in `DAGstatus_config.json`.

### copyCompress

This handles copying files from users' areas on DESY T2 to the shared group space.
In particular, it is can submit en-mass copying to the BIRD HTCondor system, since there are many files, and copying can be slow.
It attempts to put files under their respective branches, e.g. `/pnfs/desy.de/cms/tier2/store/group/uhh/uhh2ntuples/RunII_80X_v3`

_TODO: It can also optionally compress the ROOT files._

#### Setup

Compile the programs: `make`

This makes 2 programs:

- `countEvents`, which counts the number of events in an AnalysisTree (the slow way, to ensure the Tree is readable)
- `copyAndCompress`, which copies the AnalysisTree TTree from one ROOT file to another, applying maximum compression

(_We compile these since they run faster, and speed is needed for transferring the many many files._)

You will also need a valid VOMS proxy - the script will fail otherwise.

#### Running

To run on the batch system:

- first do a "dry-run", that will produce all the mappings from old -> new, without actually performing them.
```
./doCopyCompressJobs.py <XML FILENAME> --dryRun
```

One can optionally specify the `--branch XXX` argument, to help it figure out where the files should be copied to.

The script will produce an updated XML file, with the same filepath as the original, but with `.new` appended, i.e. `<XML FILENAME>.new`.
You should check the locations in this new file to ensure they look sensible (i.e. did it pick the right branch name?)

- if happy, run again, this time submitting jobs:

```
./doCopyCompressJobs.py <XML FILENAME>
```

You can check on the progress of these jobs using the `DAGstatus` tool (see above), since these jobs are run via a DAG.
Please look at the screen output, which will tell you the name of the status file.

Note that the script also checks the newly copied files to ensure they have the same number of events as the originals.
If this is not true, the job fails.

The script also produces a script, `rm_<XML FILENAME>.sh`, which commands to remove the original files.

**Only run this once all the jobs have completed successfully, and you are happy with the newly copied files.**

Good practice is to update the central XMLs, let the users use the new files for a while, then remove the originals if there are no complaints.

#### Notes

- The main script also produces a file, `mapping.txt`, with all the `<SRC>:<DEST>` entries, one per line

- `htcScript.sh` is the main script run in each job on BIRD. It iterates over all `<SRC>:<DEST>` arguments it is given

- For each `<SRC>:<DEST>` pair, it calls `copyJobScript.sh`, which actually does the copying and validation. So if you wanted to run it locally, you could do so with these scripts + `mapping.txt`

## Developer tips

If there are multiple files to a tool, please put them in a subdirectory.

For python scripts, please make them (as far as possible) python 2 and 3 compatible.
In most cases this means adding

```from __future__ import print_function```

and using `print(...)`.

More on the differences and converting:

- https://docs.python.org/3/howto/pyporting.html

- http://python-future.org/quickstart.html

One can use the `six` package to help out: https://six.readthedocs.io/


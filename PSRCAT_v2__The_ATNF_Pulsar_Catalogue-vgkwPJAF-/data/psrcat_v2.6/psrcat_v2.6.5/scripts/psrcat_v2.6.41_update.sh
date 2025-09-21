#!/bin/bash
#
# This is a template script to import upds, derive parameters and 
# convert PSRCAT v2 to v1.
# Please copy this template for a new release.
#

# Set release version here
VER=2.6.41

# Directory where the upds end up after the release
UPDS_COMPLETED=./upds_completed

# List of upds.txt files to be processed in this release
UPDS="ska_sim.txt"

# Set the logging
[ ! -e ./psrcat_update_run.log ] || /bin/rm ./psrcat_update_run.log
exec &> >(tee -a ./psrcat_update_run.log)

# Clean up the old allbibs
/bin/rm allbibs.txt

# Copy the 'stable' version to keep it untouched 
\cp psrcat2_stable.db psrcat2.db

# Insert version info to the database
echo "Insert version information"
sqlite3 psrcat2.db "INSERT INTO version (version_id,version,entryDate,notes) VALUES (NULL,'$VER',DATETIME('now'),'This update has been produced from work done by Agastya Kapur and Lawrence Toomey. \
 \
Software versions: Made by running .version in sqlite3\
SQLite 3.41.2 2023-03-22 11:56:21 0d1fc92f94cb6b76bffe3ec34d69cffde2924203304e8ffc4155597af0c191da \
zlib version 1.2.13 \
gcc-11.2.0 \
 \
Software collection DOI: https://doi.org/10.25919/nk2e-d839 \
 \
Git Commit Tag: \
 \
Update file labels: \
$UPDS
'\
)"

# Remove derived parameters from parameter table
sqlite3 psrcat2.db "DELETE FROM parameter where parameter_id IN (SELECT parameter_id FROM derived)"

# Clear derived and derivedFromParameter tables
sqlite3 psrcat2.db "DELETE FROM derived"
sqlite3 psrcat2.db "DELETE FROM derivedFromParameter"

# Do any database deletes/inserts/updates to fix previous versions here

# add survey for SKA simulated data
sqlite3 psrcat2.db "INSERT INTO survey(survey_id,entryDate,label,shortLabel,telescope,receiver) VALUES (NULL,DATETIME('now'),'Simulated SKA-Low Pulsar Survey','ska_low','SKA_Low','SKA_Low 200MHz receiver')"

# add missing content of other surveys
sqlite3 psrcat2.db "UPDATE survey SET receiver = '820MHz receiver' WHERE survey_id=42"
sqlite3 psrcat2.db "UPDATE survey SET receiver = '144MHz receiver' WHERE survey_id=40"
sqlite3 psrcat2.db "UPDATE survey SET receiver = '111MHz multibeam receiver' WHERE survey_id=39"
sqlite3 psrcat2.db "UPDATE survey SET receiver = '155MHz receiver' WHERE survey_id=38"
sqlite3 psrcat2.db "UPDATE survey SET receiver = 'L-band receiver' WHERE survey_id=37"
sqlite3 psrcat2.db "UPDATE survey SET receiver = '19-beam L-band receiver' WHERE survey_id=36"
sqlite3 psrcat2.db "UPDATE survey SET receiver = '19-beam L-band receiver' WHERE survey_id=35"







# Process the list of input upd files
echo "Processing upd input files..."
for upd in `echo $UPDS | tr ',' ' '`
do
  # sanitise the hyphens
  sed -i 's/−/-/g' $upd
  sed -i 's/–/-/g' $upd
  ./inputUpdate.tcsh -upd $upd -commit 1
done

# Do any database deletes/inserts/updates to fix retrospectively here

# Derive the parameters
echo "Deriving parameters..."
time python deriveParameters.py

# Convert to v1 output
echo "Converting from v2 to v1..."
# choose most precise parameters
time ./psrcatV2_V1 $VER psrcat2.db 1 > psrcat.db

echo "Done!"


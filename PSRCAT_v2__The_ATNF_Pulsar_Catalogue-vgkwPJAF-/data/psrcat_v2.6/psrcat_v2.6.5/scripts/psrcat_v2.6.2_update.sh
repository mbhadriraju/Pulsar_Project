#!/bin/bash
#
# Script to import upds, derive parameters and convert PSRCAT v2 to v1
#

VER=2.6.2
UPDS_COMPLETED=./upds_completed

# NOTES:
# Input cab_fixw.txt first to add widths
# 

UPDS=""

[ ! -e ./psrcat_update_run.log ] || /bin/rm ./psrcat_update_run.log
exec &> >(tee -a ./psrcat_update_run.log)

/bin/rm allbibs.txt

\cp psrcat2_stable.db psrcat2.db

echo "Insert version information"
sqlite3 psrcat2.db "INSERT INTO version (version_id,version,entryDate,notes) VALUES (NULL,'$VER',DATETIME('now'),'This update has been produced from work done by Agastya Kapur and Lawrence Toomey and others. This contains fixes for v${VER} \
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

# Change FAST discovery g names to _prov
# Add FAST names to name table
# Seems easy enough - AK
./fixGPulsars.sh 

# Process input upd files
echo "Processing upd input files..."
for upd in `echo $UPDS | tr ',' ' '`
do
  # sanitise the hyphens
  sed -i 's/−/-/g' $upd
  sed -i 's/–/-/g' $upd
  ./inputUpdate.tcsh -upd $upd -commit 1
done

# Complete ingest process
n_error=`grep -c ERROR ./psrcat_update_run.log`
n_warn=`grep -c WARN ./psrcat_update_run.log`
echo "----------------------------------"
echo "Ingest completed."
echo "Number of ERRORs:   $n_error"
if [ $n_error -ne 0 ]; then
  grep ERROR ./psrcat_update_run.log
fi
echo "Number of WARNINGs: $n_warn"
echo "----------------------------------"

# Derive parameters
echo "Deriving..."
time python deriveParameters.py

# # Convert to v1
echo "Converting from v2 to v1..."
time ./psrcatV2_V1 $VER psrcat2.db > psrcat.db

echo "Done!"


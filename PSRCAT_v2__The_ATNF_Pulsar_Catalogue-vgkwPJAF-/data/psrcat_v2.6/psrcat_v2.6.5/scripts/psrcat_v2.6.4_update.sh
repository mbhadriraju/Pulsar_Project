#!/bin/bash
#
# This is a template script to import upds, derive parameters and 
# convert PSRCAT v2 to v1.
# Please copy this template for a new release.
#

# Set release version here
VER=2.6.4

# Directory where the upds end up after the release
UPDS_COMPLETED=./upds_completed

# List of upds.txt files to be processed in this release
UPDS="adl+25.txt, cae+25.txt, hzw+25.txt, ier+16.txt, lsb+25.txt, mmr+24.txt, msk+24b.txt, sbb+25.txt, tss+25.txt, sru+25.txt, xhy+25.txt, xjx+25.txt"

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
# PSRSOFT-340
# remove hzw+25 entries
./deleteDataFromCitation.sh hzw+25

# PSRSOFT-314
# remove lsb+24 entries (ArXiv paper superseded by lsb+25)
./deleteDataFromCitation.sh lsb+24

# PSRSOFT-339

# Fix duplicate pulsar
./fixDupes.sh J0034+69 J0032+6946

# Fix hyphen error in cfc+24
sqlite3 psrcat2.db "UPDATE association SET name='MCSNR_J0100-7211' WHERE association_id=930"

# Add pulsar custom names
./addNames.sh J0835-4510 Vela
./addNames.sh J0633+1746 Geminga
./addNames.sh J0534+2200 Crab
./addNames.sh J0941-39 MeltingPot

# PSRSOFT #344 fix period error
sqlite3 psrcat2.db "UPDATE parameter SET value='0.0021147',uncertainty='0.0000001' WHERE parameter_id=185652"

# PSRSOFT #345 remove W50, W10 for J1306-4035
sqlite3 psrcat2.db "DELETE FROM parameter WHERE parameter_id=8444"
sqlite3 psrcat2.db "DELETE FROM parameter WHERE parameter_id=8445"

# PSRSOFT #346 fix incorrect W50 and W10 
# J0406+30 (mms+20)
# W50
sqlite3 psrcat2.db "UPDATE parameter SET value = 0.246 WHERE parameter_id=1942"
# W10
sqlite3 psrcat2.db "UPDATE parameter SET value = 0.638 WHERE parameter_id=1943"
# J2022+2534 (mms+20)
# W10 only
sqlite3 psrcat2.db "UPDATE parameter SET value = 1.456 WHERE parameter_id=31835"

# Fixes for flux entries with no uncertainties (bnc+24) (actually none of the fluxes have uncertainties)
#S1300    0.0700000000000000067    10   bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.07',uncertainty=NULL WHERE parameter_id=181725"
#S1300    0.1000000000000000056    7    bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.10',uncertainty=NULL WHERE parameter_id=181740"
#S1300    0.0500000000000000028    5    bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.05',uncertainty=NULL WHERE parameter_id=181798"
#S1300    0.0599999999999999978    6    bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.06',uncertainty=NULL WHERE parameter_id=181755"
#S1300    0.17000000000000001      5    bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.17',uncertainty=NULL WHERE parameter_id=181768"
#S1300    0.0599999999999999978    5    bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.06',uncertainty=NULL WHERE parameter_id=181781"
#S1300    0.119999999999999996     11   bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.12',uncertainty=NULL WHERE parameter_id=181792"

# Fixes for flux entries with no uncertainties (vcs+24)
#S816     0.050000000000000003     3    vcs+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.05',uncertainty=NULL WHERE parameter_id=115036"
#S816     0.0400000000000000008    12   vcs+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.04',uncertainty=NULL WHERE parameter_id=114970"
#S1284    0.0200000000000000004    12   vcs+24
sqlite3 psrcat2.db "UPDATE parameter SET value='0.02',uncertainty=NULL WHERE parameter_id=114971"

# Fix orbital period for J1646-4406 (zwl+24)
#PB       0.21946200000000         10   zwl+24
sqlite3 psrcat2.db "UPDATE parameter SET value='5.267104e-6',uncertainty=3e-6 WHERE parameter_id=46593"

# Add missing PEPOCHs (msk+24)
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58221 WHERE (pulsar_id=3677 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57588 WHERE (pulsar_id=3678 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59606 WHERE (pulsar_id=3679 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58510 WHERE (pulsar_id=3636 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58175 WHERE (pulsar_id=3680 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58184.4797790603843168 WHERE (pulsar_id=3681 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57470 WHERE (pulsar_id=3682 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59499 WHERE (pulsar_id=3683 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57458 WHERE (pulsar_id=3684 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58062 WHERE (pulsar_id=3634 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58521 WHERE (pulsar_id=3685 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59523 WHERE (pulsar_id=3703 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59567 WHERE (pulsar_id=3686 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59604 WHERE (pulsar_id=3687 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58051 WHERE (pulsar_id=3688 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57502 WHERE (pulsar_id=3689 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59592 WHERE (pulsar_id=3690 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58306 WHERE (pulsar_id=3643 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57618 WHERE (pulsar_id=3691 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58052 WHERE (pulsar_id=3692 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58886 WHERE (pulsar_id=3213 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59626 WHERE (pulsar_id=3693 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58220 WHERE (pulsar_id=3694 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58262 WHERE (pulsar_id=3705 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57587 WHERE (pulsar_id=3695 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58078 WHERE (pulsar_id=3644 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=57424 WHERE (pulsar_id=3696 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58221 WHERE (pulsar_id=3697 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58221 WHERE (pulsar_id=3698 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59523 WHERE (pulsar_id=3699 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59498 WHERE (pulsar_id=3700 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58319 WHERE (pulsar_id=3701 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58219 WHERE (pulsar_id=3702 AND citation_id=1215)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=58506 WHERE (pulsar_id=3637 AND citation_id=1215)"

# Add other missing PEPOCHS
#WARNING: J1638-4713 has F1 or P1, but no PEPOCH P1 4.407E-14 8 lfd+24
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59300 WHERE (pulsar_id=3749 AND citation_id=1224)"
#WARNING: J1748-2815 has F1 or P1, but no PEPOCH F1 -3.52E-13 nie+20
#sqlite3 psrcat2.db "UPDATE parameter SET referenceTime= WHERE (pulsar_id=1765 AND citation_id=369)"
# NOTE: Could not locate the PEPOCH for above
#WARNING: J1831-0941 has F1 or P1, but no PEPOCH P1 8.5612E-14 6 tsc+24
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59950 WHERE (pulsar_id=3766 AND citation_id=1231)"
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=60150 WHERE (pulsar_id=3767 AND citation_id=1231)"
#WARNING: J1858-5422 has F1 or P1, but no PEPOCH F1 -7.420E-16 5 bnc+24
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59563.356205 WHERE (pulsar_id=2527 AND citation_id=1238)"
#WARNING: J2055+1545 has F1 or P1, but no PEPOCH P1 2.019E-20 aab+22
# NOTE: Could not locate the PEPOCH for above
#WARNING: J2150-0326 has F1 or P1, but no PEPOCH F1 -6.622E-16 9 cpb+23
sqlite3 psrcat2.db "UPDATE parameter SET referenceTime=59127 WHERE (pulsar_id=3360 AND citation_id=60)"

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

# Update all zero uncertainties to NULL
sqlite3 psrcat2.db "UPDATE parameter SET uncertainty=NULL where uncertainty=0"
sqlite3 psrcat2.db "UPDATE parameter SET uncertainty=NULL where uncertainty=0.0"

# Derive the parameters
echo "Deriving parameters..."
time python deriveParameters.py

# Convert to v1 output
echo "Converting from v2 to v1..."
# choose most recent parameters
#time ./psrcatV2_V1 $VER psrcat2.db 0 > psrcat.recent.db
# choose most precise parameters
time ./psrcatV2_V1 $VER psrcat2.db 1 > psrcat.db

# Rename the g pulsars - this is run everytime we need to fix the g pulsars
# It requires the v1 database file to get the DECJ digits and compare
echo "Running ./fixGPulsars.sh..."
./fixGPulsars.sh

# Repeat convert to v1 if fixGPulsars script was run
echo "Converting from v2 to v1..."
# choose most recent parameters
#time ./psrcatV2_V1 $VER psrcat2.db 0 > psrcat.recent.db
# choose most precise parameters
time ./psrcatV2_V1 $VER psrcat2.db 1 > psrcat.db

echo "Done!"


#!/bin/bash
#
# Script to import upds, derive parameters and convert PSRCAT v2 to v1
#

VER=2.6.3
UPDS_COMPLETED=./upds_completed

UPDS="psf+22.txt,hzw+25.txt,pbhg23.txt"

[ ! -e ./psrcat_update_run.log ] || /bin/rm ./psrcat_update_run.log
exec &> >(tee -a ./psrcat_update_run.log)

/bin/rm allbibs.txt

\cp psrcat2_stable.db psrcat2.db

echo "Insert version information"
sqlite3 psrcat2.db "INSERT INTO version (version_id,version,entryDate,notes) VALUES (NULL,'$VER',DATETIME('now'),'This update has been produced from work done by Agastya Kapur and Lawrence Toomey and others. This contains fixes for ${VER} \
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

# This is a one-time update to the schema
# Prevent two different pulsars with different pulsar_ids having the same name in the name table
sqlite3 psrcat2.db "CREATE TABLE name_new (
    name_id INTEGER PRIMARY KEY,
    pulsar_id INTEGER,
    citation_id INTEGER,
    name TEXT UNIQUE,  
    entryDate TEXT,
    FOREIGN KEY (pulsar_id) REFERENCES pulsar (pulsar_id),
    FOREIGN KEY (citation_id) REFERENCES citation (citation_id)
)"

sqlite3 psrcat2.db "INSERT INTO name_new (name_id, pulsar_id, citation_id, name, entryDate) SELECT name_id, pulsar_id, citation_id, name, entryDate FROM name"

sqlite3 psrcat2.db "DROP TABLE IF EXISTS name"

sqlite3 psrcat2.db "ALTER TABLE name_new RENAME TO name"
#

# PSRSOFT #332
# Remove jmv+24 parameters due to lack of PEPOCH
# Emailed Jang and got no response
jmv_cit=`sqlite3 psrcat2.db "select citation_id from citation where v1label LIKE 'jmv+24'"`;
sqlite3 psrcat2.db "DELETE from parameter where citation_id=${jmv_cit}";
sqlite3 psrcat2.db "DELETE from linkedSet where citation_id=${jmv_cit}";

# PSRSOFT #290
# Resolves whx+23 and wys+24
python fixWidths.py > widthcmd.sql
source widthcmd.sql

# PSRSOFT #336
J1884_id=`sqlite3 psrcat2.db "select pulsar_id from pulsar where jname LIKE 'J1848-0129A'"`;
mmr_cit=`sqlite3 psrcat2.db "select citation_id from citation where v1label LIKE 'mmr+24'"`;
sqlite3 psrcat2.db "DELETE from parameter where (citation_id=${mmr_cit} and pulsar_id NOT IN (${J1848_id}))";

# Small bug fix for variable
# Fix up citation for name table when forgot $ variable
sqlite3 psrcat2.db "UPDATE name SET citation_id=1161 WHERE name_id=538"

# PSRSOFT #
# UPDATE names to have new names in database
# These 3 had long names in the name table and short names in the pulsar table
# This has now been reversed by running the lines below
J0043_jnameid=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J0043-73'"`
J0043_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J0043-73'"`
J0043_nameid=`sqlite3 psrcat2.db "SELECT name_id FROM name WHERE name LIKE 'J0043-7319'"`
sqlite3 psrcat2.db "UPDATE pulsar SET jname='J0043-7319' WHERE pulsar_id=${J0043_jnameid}"
sqlite3 psrcat2.db "UPDATE name SET name='J0043-73',citation_id='${J0043_citid}' WHERE name_id=${J0043_nameid}"

J0052_jnameid=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J0052-72'"`
J0052_nameid=`sqlite3 psrcat2.db "SELECT name_id FROM name WHERE name LIKE 'J0051-7204'"`
J0052_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J0052-72'"`
sqlite3 psrcat2.db "UPDATE pulsar SET jname='J0051-7204' WHERE pulsar_id=${J0052_jnameid}"
sqlite3 psrcat2.db "UPDATE name SET name='J0052-72',citation_id='${J0052_citid}' WHERE name_id=${J0052_nameid}"

J1843_jnameid=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J1843-08'"`
J1843_nameid=`sqlite3 psrcat2.db "SELECT name_id FROM name WHERE name LIKE 'J1843-0757'"`
J1843_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J1843-08'"`
sqlite3 psrcat2.db "UPDATE pulsar SET jname='J1843-0757' WHERE pulsar_id=${J1843_jnameid}"
sqlite3 psrcat2.db "UPDATE name SET name='J1843-08',citation_id='${J1843_citid}' WHERE name_id=${J1843_nameid}"

# Replace all duplicate names found
#./fixDupes.sh origname newname 
./fixDupes.sh J0957-06 J0957-0619
./fixDupes.sh J1239+32 J1239+3239
./fixDupes.sh J1505-25 J1505-2524
./fixDupes.sh J1530-21 J1530-2114
./fixDupes.sh J1929+66 J1929+6630
./fixDupes.sh J2055+15 J2055+1545
./fixDupes.sh J2145+21 J2145+2158
./fixDupes.sh J2210+57 J2210+5712
./fixDupes.sh J2354-22 J2354-2250 
./fixDupes.sh J1652-42 J1652-4237

# Dealing with J1655-40 and J1655-401 differently. sbb+23 published the name -40 that was duplicate name, so we made it -401. Then they republished the same pulsar with -40 and we missed the duplicate.
J1655401_id=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J1655-401'"`
J165540_id=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J1655-40'"`
J1655401_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J1655-401'"`
J165540_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J1655-401'"`
sqlite3 psrcat2.db "UPDATE parameter SET pulsar_id=${J1655401_id} WHERE pulsar_id=${J165540_id}"
sqlite3 psrcat2.db "INSERT INTO name (name_id,pulsar_id,citation_id,name,entryDate) VALUES (NULL,'${J1655401_id}','${J165540_citid}','J1655-40',DATETIME('now'))"
sqlite3 psrcat2.db "DELETE FROM pulsar WHERE pulsar_id=${J165540_id}"

# Cases where these two pulsars were already in with long names and we put parameters in with short names...
J05534111_id=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J0553+4111'"`
J055341_id=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J0553+41'"`
J05534111_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J0553+4111'"`
J055341_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J0553+41'"`
sqlite3 psrcat2.db "UPDATE parameter SET pulsar_id=${J05534111_id} WHERE pulsar_id=${J055341_id}"
sqlite3 psrcat2.db "INSERT INTO name (name_id,pulsar_id,citation_id,name,entryDate) VALUES (NULL,'${J05534111_id}','${J055341_citid}','J0553+41',DATETIME('now'))"
sqlite3 psrcat2.db "DELETE FROM pulsar WHERE pulsar_id=${J055341_id}"

J21163701_id=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J2116+3701'"`
J211637_id=`sqlite3 psrcat2.db "SELECT pulsar_id FROM pulsar WHERE jname LIKE 'J2116+37'"`
J21163701_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J2116+3701'"`
J211637_citid=`sqlite3 psrcat2.db "SELECT citation_id FROM pulsar WHERE jname LIKE 'J2116+37'"`
sqlite3 psrcat2.db "UPDATE parameter SET pulsar_id=${J21163701_id} WHERE pulsar_id=${J211637_id}"
sqlite3 psrcat2.db "INSERT INTO name (name_id,pulsar_id,citation_id,name,entryDate) VALUES (NULL,'${J21163701_id}','${J211637_citid}','J2116+37',DATETIME('now'))"
sqlite3 psrcat2.db "DELETE FROM pulsar WHERE pulsar_id=${J211637_id}"

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

# Convert to v1
echo "Converting from v2 to v1..."
time ./psrcatV2_V1 $VER psrcat2.db > psrcat.db

# Rename the g pulsars - this is run everytime we need to fix the g pulsars
# It requires the v1 database file to get the DECJ digits and compare
echo "Running ./fixGPulsars.sh..."
./fixGPulsars.sh

# Repeat convert to v1 if fixGPulsars script was run
echo "Converting from v2 to v1..."
time ./psrcatV2_V1 $VER psrcat2.db > psrcat.db

echo "Done!"


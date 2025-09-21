#!/bin/tcsh 

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au

# Set variables to empty strings for checking later on
set paperInfo = ""
set delim = ""
set commit = ""
set database = ""

set numargs = ${#argv}
set arg = 0
set val = 1

# Read arguments and assign variables
while ( $arg <= ($numargs - 1) )

    if ( "$argv[$arg]" == "-upd" ) then
	set paperInfo="$argv[$val]"
    else if ( "$argv[$arg]" == "-delim" ) then
	set delim="$argv[$val]"
    else if ( "$argv[$arg]" == "-commit" ) then
	set commit="$argv[$val]"
    else if ( "$argv[$arg]" == "-database" ) then
	set database="$argv[$val]"
    endif
    @ arg = ($arg + 1)
    @ val = ($val + 1)
end

# If no input file, then exit
if ( $paperInfo == "" ) then
    echo "NO PAPER ENTERED"
    echo "RUN AS ./inputUpdate -upd <file.txt>"
    echo "THIS WILL USE DELIMITER = ',' AND COMMIT = 0 AND DATABASE = psrcat.db"
    echo "If different delimiter and want commit and choose database"
    echo "RUN AS ./inputUpdate -upd <file.txt> -delim <delim> -commit 1 -database <database.db>"
    exit
endif

# If no delimiter, then set delim to ','
if ( $delim == "" ) then
    set delim = ","
endif

# If no commit, then set commit to 0. i.e. Do not commit
if ( $commit == "" ) then
    set commit = "0"
endif

# If no database name given, use psrcat2.db
if ( $database == "" ) then
    set database = "psrcat2.db"
endif

echo "Processing $paperInfo"

# Check if there are any hyphens in the text and return error message if there are
set numHyphen = `grep -e '−' -e '–' $paperInfo | wc -l`
if ( $numHyphen == "0" ) then
    echo "There are no hyphens. Proceeding"
else
    echo "There are $numHyphen hyphens in the upd file. Please fix"
    grep '−' $paperInfo
    grep '–' $paperInfo
    echo "Exiting"
    exit
endif

# Make temporary file removing '#'s
cat $paperInfo | grep -v '#' >! ${paperInfo}F
python parseBib.py ${paperInfo}F $database >! t1
set citID=`tail -1 t1`
echo "Citation done"
\rm ${paperInfo}F

# Extract each GROUP into its own file because Agastya couldn't figure out how to parse them all together
./extract.tcsh $paperInfo

# Input each group together. 
foreach file (`ls *-$paperInfo.dat | sort -V`)
    echo "--- Processing $file now ---"
    python addGroup.py $file $citID $delim $commit $database | tee $file.temp
    if (`grep -ci error $file.temp` =~ "0") then
      if ( $commit != "1" ) then
          # Delete citation if no commit. Other data doesn't get added through the other python code
	  sqlite3 psrcat2.db "DELETE FROM citation where citation_id=$citID"
      endif
    else
      echo "ERROR: Error processing $file! See ./updateLogs/$file.temp"
      exit 1
    endif
    echo "--- Parameters for $file done ---\n"
end

\rm *-$paperInfo.dat
\rm t1

# Move logs to updateLogs
mkdir -p updateLogs
mv *.temp updateLogs
echo "--- Parameters $paperInfo done ---\n"

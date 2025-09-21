#!/bin/tcsh 

# Copyright: CSIRO 2024
# Author: Lawrence Toomey: Lawrence.Toomey@csiro.au, Agastya Kapur: Agastya.Kapur@csiro.au


set psrinfo = $1

if ( ! -e $psrinfo ) then
  echo "ERROR: $psrinfo not found. Exiting."
  exit 1
endif
 
set ncollections = `grep -cE "^GROUP" $psrinfo`

foreach i (`seq 1 1 $ncollections`)
  echo "## Found GROUP $i...writing to $i-$psrinfo.dat ##"
  awk -v n=$i '/^GROUP/{l++} (l==n){print}' $psrinfo | grep -v '#' > $i-$psrinfo.dat
end


#!/bin/sh
if [ $# -ne 1 ]; then
  cat <<EOS
Copy Amedas raw data to CWD and preprocess them.

Usage: $ ./amedas-preprocess.sh /path/to/amedas/raw/
EOS
  exit 1
fi

set -eux

cp $1/*.csv .

# remove unnecessary header lines
gsed -i '1,3d' *.csv
gsed -i '2d' *.csv

# remove unnecessary data lines
gsed -i '/^2021\/12\/31/d' 20220101*.csv
gsed -i '/^2022\/2\/1/d' 20220101*.csv
gsed -i '/^2022\/7\/31/d' 20220801*.csv
gsed -i '/^2022\/9\/1/d' 20220801*.csv

# SJIS -> UTF-8
perl -i -MEncode -pe 's/\r\n/\n/g;Encode::from_to($_,"shiftjis","utf8");' *.csv

# CSV -> TSV
gsed -i s/,/\\t/g *.csv
for f in $(ls *.csv) ; do f_base=$(basename $f .csv) ; mv $f ${f_base}.tsv ; done

# Timestamp field into RFC-3336 (2022-01-01T00:00:00.000+09:00)
gsed -i "s|\(.*\)/\(.*\)/\(.*\) \(.*\):\(.*\):\(.*\)|\1-\2-\3T\4:\5:\6|g" *.tsv
gsed -i "s|00:00\\t|00:00.000+09:00\\t|g" *.tsv
gsed -i "s|2022-\(.\)-|2022-0\1-|g" *.tsv
gsed -i "s|-\([0-9]\)T|-0\1T|g" *.tsv
gsed -i "s|T\([0-9]\):|T0\1:|g" *.tsv

# Remove unnecessary columns
gawk -i inplace 'BEGIN {OFS="\t"} {$3=""; $4=""; $6=""; $7=""; $8="" ; print}' *.tsv
gsed -i "s|\\t\+|\\t|g" *.tsv
gsed -i "s|\\t*$||g" *.tsv

# Rename column
gsed -i "s|年月日時|timestamp|g" *.tsv
gsed -i "s|気温(℃)|temperature [°C]|g" *.tsv
gsed -i "s|降水量(mm)|rainfall [mm]|g" *.tsv

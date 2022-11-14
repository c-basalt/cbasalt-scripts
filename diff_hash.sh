#!/bin/bash
echo "-----------"
echo tail $1
tail -n3 $1
echo tail $2
tail -n3 $2
echo
echo only in $1
awk 'NR==FNR{c[$1]++;next};c[$1] == 0' "$2" "$1"
echo
echo only in $2
awk 'NR==FNR{c[$1]++;next};c[$1] == 0' "$1" "$2"
echo

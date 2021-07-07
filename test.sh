#!/bin/bash
for((i=0;i<9;i++))
do
  echo "i is $i"
  tmpdate=$(date -d "$i days" "+%b %d")
  echo $tmpdate
done
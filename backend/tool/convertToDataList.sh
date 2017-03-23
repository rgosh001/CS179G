#!/bin/bash

if [ -e ""$1"_datalist" ]; then
  rm ""$1"_datalist";
fi

touch ""$1"_datalist"
echo ""$1"_datalist"

echo "<datalist id=\"languages\">" >> ""$1"_datalist"

while IFS= read -r var
do
  echo "    <option value = \""$var"\">" >> ""$1"_datalist"
done < "$1"

echo "</datalist>" >> ""$1"_datalist"


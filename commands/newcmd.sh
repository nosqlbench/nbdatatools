#!/bin/bash
export CMDNAME=${CMDNAME:?Must provide CMDNAME var}
export FORCE=${FORCE:-false}
printf "creating command template module:\n"
printf "CMDNAME=%s\n" "${CMDNAME}"
printf "FORCE=%s\n" "${FORCE}"

if [ -e "${CMDNAME}" ]
then
 if [ "${FORCE}" != "true" ]
 then
  printf "%s exists and FORCE is %s aborting\n" "${CMDNAME}" "${FORCE}"
  exit 2
 fi
fi
rsync -av --delete template/ ${CMDNAME}/

find ${CMDNAME} -type d -name COMMAND -print | while read dirname
do
 NEWDIR=${dirname/COMMAND/${CMDNAME}}
 echo "DIRNAME $dirname" 
 echo "NEWDIR $NEWDIR" 
 mv "${dirname}" "${NEWDIR}"
done
find ${CMDNAME} -type f | xargs grep -l 'COMMAND' | while read filename
do
 echo "FILENAME $filename"
 perl -pi -e 's/COMMAND/$ENV{CMDNAME}/g' ${filename}
done


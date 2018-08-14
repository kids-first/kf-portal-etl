!# /bin/bash

FILE=$1

if [ ! -e $FILE  ]; then
    echo "The file '$FILE' does not exist"
fi

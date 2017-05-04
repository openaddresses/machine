#!/bin/sh -e

if [ $# != 1 ]; then
    echo "Usage: $0 <commit message>"
    exit 1
fi

MESSAGE=$1
DIR=`dirname $0`
VERSION=`cat $DIR/../openaddr/VERSION`
MAJOR="`cut -f1 -d. $DIR/../openaddr/VERSION`.x"

echo "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
echo "Committing $VERSION and pushing to $MAJOR..."
echo "-------------------------------------------------------------"
git commit -m "Bumped to $VERSION with $MESSAGE"
git tag $VERSION
git push --tags
git push origin $VERSION:$MAJOR
git push origin master

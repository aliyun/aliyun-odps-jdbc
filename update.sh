#!/bin/bash
VERSION=$1
TAG_VERSION=$2
SDK_VERSION=$3
if [ $# -lt 3 ]; then
    echo "Usage: ./update.sh VERSION TAG_VERSION SDK_VERSION"
    exit -1
fi
echo "VERSION: "$VERSION
echo "TAG_VERSION: "$TAG_VERSION
echo "SDK_VERSION: "$SDK_VERSION
echo "you have 5 seconds to cancel"
sleep 5

commit_msg=""

if [ $SDK_VERSION != "0" ]; then
    sed -i '' "s#<sdk.version>.*</sdk.version>#<sdk.version>$SDK_VERSION</sdk.version>#g" pom.xml
    commit_msg="update SDK version to $SDK_VERSION "
fi
if [ $VERSION != "0" ]; then
    mvn versions:set -DnewVersion=$VERSION versions:commit
    commit_msg=$commit_msg"update version to $VERSION"
fi

echo $commit_msg

git commit -a -m "$commit_msg"
git push
git tag $TAG_VERSION -f
git push --tags -f
#!/bin/bash
set -e
rm -rf build 2> /dev/null
rm -rf temp 2> /dev/null
mkdir temp
cp -rf source/* temp/
# replace relative URLs with absolute URLs
sed -i 's/\.\.\/\.\.\/\.\.\//https:\/\/github.com\/apache\/arrow-datafusion\/blob\/master\//g' temp/contributor-guide/index.md
make SOURCEDIR=`pwd`/temp html

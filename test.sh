#!/bin/sh

set -e

cargo build

echo "Cleaning up any remains"

rm -rf testing

echo "Preparing a test repository"

mkdir testing
cd testing

git init source

echo "test" > source/file

(cd source; git add file; git commit -m initial)

git init --bare target

echo "Having a go at transferring"

set -x

cargo run -- $(cd source; pwd) $(cd target; pwd)

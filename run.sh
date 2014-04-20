#!/bin/bash

echo "data" >  /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt
echo "data" >> /tmp/tiny_file.txt

ddfs push text /tmp/tiny_file.txt

go build examples/simple_job/main.go

ddfs blobs text \
    | disco job -m -r -p godisco_test ./main \
    | xargs disco wait \
    | xargs ddfs cat

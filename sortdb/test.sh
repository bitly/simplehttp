#!/bin/sh

testsubdir=test_output
rm -rf "$testsubdir" > /dev/null 2>&1
mkdir -p "$testsubdir"

CMP="${CMP-cmp}"

ln -s -f test.tab test.db
./sortdb -f test.db -a 127.0.0.1 -p 8080 2>/dev/null 1>/dev/null &
sleep 1
for key in a b c m o zzzzzzzzzzzzzzzzzzzzzzzz zzzzzzzzzzzzzzzzzzzzzzzzz zzzzzzzzzzzzzzzzzzzzzzzzzz; do 
    echo "/get?key=$key" >> $testsubdir/test.out
    curl --silent "localhost:8080/get/?key=$key" >> $testsubdir/test.out
done

# now swap the db and check keys again
ln -s -f test2.tab test.db
kill -s HUP %1

echo "/get?key=a (should be a new key 'new db')" >> $testsubdir/test.out
curl --silent "localhost:8080/get/?key=a" >> $testsubdir/test.out
echo "/get?key=b not found" >> $testsubdir/test.out
curl --silent "localhost:8080/get/?key=b" >> $testsubdir/test.out

err=0;
if ! "$CMP" -s "test.expected" "${testsubdir}/test.out" ; then
	echo "ERROR: test failed:" 1>&2
	diff "test.expected" "${testsubdir}/test.out" 1>&2
	err=1
else
    echo "TEST PASSED"
fi

kill %1
exit $err;

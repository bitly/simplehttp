#!/bin/sh

testsubdir=test_output
rm -rf "$testsubdir" > /dev/null 2>&1
mkdir -p "$testsubdir"

CMP="${CMP-cmp}"

./sortdb -f test.tab -a 127.0.0.1 -p 8080 2>/dev/null 1>/dev/null &
sleep 1
for key in a b c m o zzzzzzzzzzzzzzzzzzzzzzzz zzzzzzzzzzzzzzzzzzzzzzzzz zzzzzzzzzzzzzzzzzzzzzzzzzz; do 
    echo "/get?key=$key" >> $testsubdir/test.out
    curl --silent "localhost:8080/get/?key=$key" >> $testsubdir/test.out
done

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

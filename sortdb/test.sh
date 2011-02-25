#!/bin/sh

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=`dirname "$SCRIPT"`
testsubdir=test_output
rm -rf "$testsubdir" > /dev/null 2>&1
mkdir -p "$testsubdir"

CMP="${CMP-cmp}"

run_vg (){
    TEST_COMMAND="$1"
    TEST_OPTIONS="$2"
    REDIR_OUTPUT="2>/dev/null 1>/dev/null"
    # REDIR_OUTPUT=""
    eval valgrind --tool=memcheck \
        --trace-children=yes \
        --demangle=yes \
        --log-file="${testsubdir}/vg.out" \
        --leak-check=full \
        --show-reachable=yes \
        --run-libc-freeres=yes \
    "\"${SCRIPTPATH}/${TEST_COMMAND}\"" $TEST_OPTIONS ${REDIR_OUTPUT} &
}
err=$?

ln -s -f test.tab test.db
run_vg sortdb "-f test.db -a 127.0.0.1 -p 8080"
sleep 1
for key in a b c m o zzzzzzzzzzzzzzzzzzzzzzzz zzzzzzzzzzzzzzzzzzzzzzzzz zzzzzzzzzzzzzzzzzzzzzzzzzz; do 
    echo "/get?key=$key" >> $testsubdir/test.out
    curl --silent "localhost:8080/get/?key=$key" >> $testsubdir/test.out
done
echo "/mget?k=a&k=c&k=o" >> $testsubdir/test.out
curl --silent "localhost:8080/mget?k=a&k=c&k=o" >> $testsubdir/test.out

# now swap the db and check keys again
ln -s -f test2.tab test.db
curl --silent "localhost:8080/reload" >> $testsubdir/test.out

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
    echo "FUNCTIONAL TEST PASSED"
fi

curl --silent "localhost:8080/exit"
sleep .5;
if ! grep -q "ERROR SUMMARY: 0 errors" "${testsubdir}/vg.out" ; then
    echo "ERROR: valgrind found errors during execution:" 1>&2
    cat "${testsubdir}/vg.out"
    err=1
fi
if ! grep -q "definitely lost: 0 bytes in 0 blocks" "${testsubdir}/vg.out" ; then
    echo "ERROR: valgrind found leaks during execution:" 1>&2
    cat "${testsubdir}/vg.out"
    err=1
fi
if ! grep -q "possibly lost: 0 bytes in 0 blocks" "${testsubdir}/vg.out" ; then
    echo "ERROR: valgrind found leaks during execution:" 1>&2
    cat "${testsubdir}/vg.out"
    err=1
fi

exit $err;

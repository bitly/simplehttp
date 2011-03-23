#!/bin/sh

if [ ! -f simpletokyo ]; then
    make
fi

if [ -f /tmp/simpletokyo_test.tcb ]; then
    rm -rf /tmp/simpletokyo_test.tcb ;
fi

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


OUT=$testsubdir/test.out

ttserver -host 127.0.0.1 -port 8079 -thnum 1 /tmp/simpletokyo_test.tcb 2>/dev/null 1>/dev/null &
run_vg simpletokyo "-A 127.0.0.1 -P 8079 -a 127.0.0.1 -p 8080"

sleep 1;

for k in a b c
do
    # put 100 entries per prefix
    for (( i = 1000; i <= 1100; i++ ))
    do
        curl --silent "http://localhost:8080/put?key=$k$i&value=$i" >/dev/null
    done

    # delete even entries
    for (( i = 1000; i <= 1100; i+=2 ))
    do
        curl --silent "http://localhost:8080/del?key=$k$i" >/dev/null
    done
done

# get a few specific keys
echo "key should not exist" >> ${OUT}
curl --silent "http://localhost:8080/get?key=a1000"  >> ${OUT}
echo "key should exist"  >> ${OUT}
curl --silent "http://localhost:8080/get?key=a1001"  >> ${OUT}
echo "should return 1005, 1007, 1009"  >> ${OUT}
curl --silent "http://localhost:8080/fwmatch?key=a&length=3&offset=2"  >> ${OUT}
echo "set of odd data"  >> ${OUT}
curl --silent "http://localhost:8080/put?key=odd&value=%3C%3E%26this%3Dthat%7B%7D%20%2B*"  >> ${OUT}
echo "should == '<>&this=that{} +*'"  >> ${OUT}
curl --silent "http://localhost:8080/get?key=odd"  >> ${OUT}
curl --silent "http://localhost:8080/del?key=odd"  >> ${OUT}

echo "should == '     ' not '+++++'" >> ${OUT}
curl --silent "http://localhost:8080/put?key=plus&value=++++++" >> ${OUT}
curl --silent "http://localhost:8080/get?key=plus" >> ${OUT}
curl --silent "http://localhost:8080/del?key=plus" >> ${OUT}

echo "key should not exist" >> ${OUT}
curl --silent "http://localhost:8080/get?key=incr_test" >> ${OUT}
curl --silent "http://localhost:8080/incr?key=incr_test" >> ${OUT}
curl --silent "http://localhost:8080/incr?key=incr_test&value=5" >> ${OUT}
echo "value should be 6" >> ${OUT}
curl --silent "http://localhost:8080/get_int?key=incr_test" >> ${OUT}

echo "incr on two different keys at the same time. both values should be == 1" >> ${OUT}
curl --silent "http://localhost:8080/incr?key=double_key1&key=double_key2" >> ${OUT}
curl --silent "http://localhost:8080/get_int?key=double_key1" >> ${OUT}
curl --silent "http://localhost:8080/get_int?key=double_key2" >> ${OUT}

echo "set key, get key, vanish db, and get key" >> ${OUT}
curl --silent "http://localhost:8080/put?key=vanishtest&value=asdf" >> ${OUT}
curl --silent "http://localhost:8080/get?key=vanishtest" >> ${OUT}
curl --silent "http://localhost:8080/vanish" >> ${OUT}
echo "now no key" >> ${OUT}
curl --silent "http://localhost:8080/get?key=vanishtest" >> ${OUT}

err=0;
if ! "$CMP" -s "test.expected" "${testsubdir}/test.out" ; then
    echo "ERROR: test failed:" 1>&2
    diff -U 3 "test.expected" "${testsubdir}/test.out" 1>&2
    err=1
else
    echo "FUNCTIONAL TEST PASSED"
fi

kill %1 # ttserver
curl --silent "http://localhost:8080/exit" >> ${OUT}

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

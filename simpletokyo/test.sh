#!/bin/sh

if [ ! -f simpletokyo ]; then
    make
fi

if [ -f /tmp/simpletokyo_test.tcb ]; then
    rm -rf /tmp/simpletokyo_test.tcb ;
fi

testsubdir=test_output
rm -rf "$testsubdir" > /dev/null 2>&1
mkdir -p "$testsubdir"
CMP="${CMP-cmp}"

OUT=$testsubdir/test.out

ttserver -host 127.0.0.1 -port 8079 -thnum 1 /tmp/simpletokyo_test.tcb 2>/dev/null 1>/dev/null &
./simpletokyo -A 127.0.0.1 -P 8079 -a 127.0.0.1 -p 8080 2>/dev/null 1>/dev/null &

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

err=0;
if ! "$CMP" -s "test.expected" "${testsubdir}/test.out" ; then
	echo "ERROR: test failed:" 1>&2
	diff "test.expected" "${testsubdir}/test.out" 1>&2
	err=1
else
    echo "TEST PASSED"
fi

kill %1
kill %2
exit $err;

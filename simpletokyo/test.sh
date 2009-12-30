
for k in a b c
do
    # put 100 entries per prefix
    for (( i = 1000; i <= 1100; i++ ))
    do
        curl "http://localhost:8080/put?key=$k$i&value=$i"
    done

    # delete even entries
    for (( i = 1000; i <= 1100; i+=2 ))
    do
        curl "http://localhost:8080/del?key=$k$i"
    done
done
# get a few specific keys
echo "key should not exist"
curl "http://localhost:8080/get?key=a1000"
echo "key should exist"
curl "http://localhost:8080/get?key=a1001"
echo "should return 1005, 1007, 1009"
curl "http://localhost:8080/fwmatch?key=a&length=3&offset=2"
echo "set of odd data"
curl "http://localhost:8080/put?key=odd&value=%3C%3E%26this%3Dthat%7B%7D%20+*"
echo "should == '<>&this=that{} +*'"
curl "http://localhost:8080/get?key=odd"
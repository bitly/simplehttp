
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
curl "http://localhost:8080/get?key=a1000"
curl "http://localhost:8080/get?key=a1001"
curl "http://localhost:8080/fwmatch?key=a&max=10"


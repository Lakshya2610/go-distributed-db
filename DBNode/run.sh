PORT=5000
NODES=$1

go build .
rm *.log

idx=0
while [ $idx -lt $NODES ]
do
    echo "Launching node $idx with port $PORT"
    ./DBNode -id $idx -port $PORT -controller "http://localhost:3000" &
    PORT="$(( $PORT + 1 ))"
    ((idx++))
done

# cargo build

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33968 &
export backend1_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33969 &
export backend2_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33970 &
export backend3_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33971 &
export backend4_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33972 &
export backend5_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33973 &
export backend6_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33974 &
export backend7_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33975 &
export backend8_pid=$!

/home/abhishek/ucsd/cse_223b_distsys/labs/lab-3-qyav/target/debug/kv-server -a localhost:33976 &
export backend9_pid=$!

read userinput

kill -9 $backend1_pid
echo "Killed pid $backend1_pid"
kill -9 $backend2_pid
echo "Killed pid $backend2_pid"
kill -9 $backend3_pid
echo "Killed pid $backend3_pid"
kill -9 $backend4_pid
echo "Killed pid $backend4_pid"
kill -9 $backend5_pid
echo "Killed pid $backend5_pid"
kill -9 $backend6_pid
echo "Killed pid $backend6_pid"
kill -9 $backend7_pid
echo "Killed pid $backend7_pid"
kill -9 $backend8_pid
echo "Killed pid $backend8_pid"
kill -9 $backend9_pid
echo "Killed pid $backend9_pid"

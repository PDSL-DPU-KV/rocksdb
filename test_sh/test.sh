for i in 0 1 2 3 4 5 6 7 ; do
echo $i
./test_single.sh $i > test$i.out &
done 
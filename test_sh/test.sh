#for i in 0 1 2 3 4 5 6 7; do
for i in 0; do
echo $i
./test_single.sh $i > test$i.out &
done 


for i in $(seq 4 10);
    do
	start=`date +%s`
	a=$(spark-submit --master yarn --deploy-mode cluster  --executor-memory 2G --num-executors clustering.py /datasets/gutenberg-txt-es/ $i 2000 spanish /user/emonto15/out$i)
  end=`date +%s`
  runtime=$((end-start))
  echo "Time with K=$i was $runtime"
    done

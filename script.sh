
for i in $(seq 4 10);
    do
	start=`date +%s`
	spark-submit clustering.py /datasets/gutenberg-txt-es/ $i 2000 spanish /user/emonto15/out$i >  /dev/null 2>&1
  end=`date +%s`
  runtime=$((end-start))
  echo "Time with K=$i was $runtime"
    done

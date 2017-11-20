
for i in $(seq 4 10);
    do
	spark-submit clustering.py /datasets/gutenberg-txt-es/ $i 2000 spanish /user/emonto15/out$1
    done

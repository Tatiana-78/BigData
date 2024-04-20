#!/bin/bash
import sys
spark-submit --master=yarn --num-executors=1 sparkdf_task1.py sys.argv[1]

#!/bin/bash
out_dir=$1
spark-submit sparkdf_task1.py $out_dir

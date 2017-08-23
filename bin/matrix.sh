hadoop jar build/libs/hadoopStudy.jar mr.matrix.MatrixDriver -Dmapreduce.task.profile=true \
    -Dmapreduce.task.profile.params="-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s" \
    -Dmapreduce.task.profile.maps="0-2" -Dmapreduce.task.profile.reduces=""


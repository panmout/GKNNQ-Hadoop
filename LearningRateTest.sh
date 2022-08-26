# Run like this:
# hadoop jar gnn.jar gr.uth.ece.dsel.preliminary.LearningRateTest <namenode> <querydir> <querydataset> <step> <mindist> <counter_limit> <pointdist>
hadoop jar ./target/gknn-hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop.preliminary.LearningRateTest \
nameNode=panagiotis-lubuntu \
queryDir=input \
queryDataset=query-dataset.txt \
step=0.00001 \
minDist=10 \
counter=100 \
diff=0.00001

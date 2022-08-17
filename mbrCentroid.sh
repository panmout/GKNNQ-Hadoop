# execute (<namenode name> <hdfs dir name> <query dataset> <hdfs GNN dir name> <step> <mindist> <counter_limit> <pointdist>)

hadoop jar ./target/gknn-hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop.preliminary.MbrCentroid \
nameNode=Hadoopmaster \
queryDir=input \
queryDataset=linearwaterNNew_sub_2.8M.txt \
gnnDir=gnn \
step=0.00000001 \
minDist=10 \
counter_limit=500 \
diff=0.00000001

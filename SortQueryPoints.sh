# execute (<namenode name> <hdfs dir name> <query dataset>)

hadoop jar ./target/hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop.preliminary.SortQueryPoints \
nameNode=panagiotis-lubuntu \
queryDir=input \
queryDataset=query-dataset.txt

###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=HadoopStandalone
trainingDir=input
treeDir=sampletree
trainingDataset=all_buildingsNNew_obj_onepercent.txt
samplerate=100
capacity=30

###########################################################################
#                                    EXECUTE                              # ###########################################################################

hadoop jar ./target/gknn-hadoop-0.0.1-SNAPSHOT.jar \
gr.uth.ece.dsel.hadoop.preliminary.QuadtreeArray \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity

datasetDir=input
datasetName=all_buildingsNNew_obj.txt
outputPath=datasetSample
samplerate=1
reducers=1

hadoop jar ./target/gknn-hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop.util.takeSample.SampleDriver $datasetDir/$datasetName $outputPath $samplerate $reducers

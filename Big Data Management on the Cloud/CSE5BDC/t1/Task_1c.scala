// Load the input data and split each line into an array of strings
val vgdataLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t1/vgsales.csv")
val vgdata = vgdataLines.map(_.split(";"))

// Task 1c:
// TODO: *** Put your solution here ***
def calculateGlobalSales (na:String, eu:String, j:String) : Double = {
    na.toDouble + eu.toDouble + j.toDouble
}
val vgdataGlobal = vgdata.map(r => (r(3), calculateGlobalSales(r(5),r(6),r(7)))).reduceByKey(_+_)

val vgdataMax = vgdataGlobal.reduce((x,y) => { 
  if(x._2 < y._2) y 
  else x
})

val vgdataMin = vgdataGlobal.reduce((x,y) => { 
  if(x._2 < y._2) x 
  else y
})


println("Highest selling Genre: " + vgdataMax._1 + "   Global Sale (in millions): " + vgdataMax._2)
println("Lowest selling Genre: " + vgdataMin._1 + "   Global Sale (in millions): " + vgdataMin._2)
    
    
// Required to exit the spark-shell
sys.exit(0)

// ERROR LiveListenerBus: Listener EventLoggingListener threw an exception
// java.io.IOException: Failed to replace a bad datanode on the existing pipeline due to no more good datanodes being available to try. (Nodes: current=[DatanodeInfoWithStorage[10.142.1.2
// :50010,DS-0c9baf0b-c1f9-46c2-b5f8-aa5bb4e6c442,DISK], DatanodeInfoWithStorage[10.142.1.1:50010,DS-c5771155-beda-4d3b-91ec-ed95cad48cee,DISK]], original=[DatanodeInfoWithStorage[10.142.
// 1.2:50010,DS-0c9baf0b-c1f9-46c2-b5f8-aa5bb4e6c442,DISK], DatanodeInfoWithStorage[10.142.1.1:50010,DS-c5771155-beda-4d3b-91ec-ed95cad48cee,DISK]]). The current failed datanode replaceme
// nt policy is DEFAULT, and a client may configure this via 'dfs.client.block.write.replace-datanode-on-failure.policy' in its configuration.
//         at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.findNewDatanode(DFSOutputStream.java:1042)
//         at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.addDatanode2ExistingPipeline(DFSOutputStream.java:1116)
//         at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.setupPipelineForAppendOrRecovery(DFSOutputStream.java:1274)
//         at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.processDatanodeError(DFSOutputStream.java:999)
//         at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.run(DFSOutputStream.java:506)
// vgdataMax: (String, Double) = (Shooter,27.57)
package eeg.anomd

import kafka.producer._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer._

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel    
import org.apache.log4j.Logger

import java.text.SimpleDateFormat
import java.util.Date

import scala.math

/**
 * Find Anomaly in the stream coming through Kafka
 * Usage: StreamAnomalyDetector <Kafka-topic> . . . . 
 */

object StreamAnomalyDetector {

def main(args: Array[String]) {
        if (args.length < 1) {
                System.err.println("Usage: StreamAnomalyDetector <First-topic>")
                System.exit(1)
                }       
	// Comparing Windowed AmplitudeTopK with CMA and if Windowed AmplitudeTopKA is more than CMA * 2.7 for a period of 3 windows, then its a seizure 
	val updateSum = (values: Seq[(Double,Int,Int)], state: Option[(Double,Int,Int)]) => Option[(Double,Int,Int)] {

        	val currentNumerator : Double = values.map(_._1).sum
                val currentDenominator = values.map(_._2).sum
		val currentAnomalyCounter = values.map(_._3).sum
		val (previousNumerator, previousDenominator, previousAnomalyCounter)  = state.getOrElse(0.0,0,0)
               	
		val currentCMA : Double = (currentNumerator + previousNumerator)/(currentDenominator + previousDenominator) 
		var a = 0.0; var b = 0; var c = 0; var d = 0.0; var e = 0.0; 
		
                if (  ((currentDenominator + previousDenominator) <= 1000) || ( ((currentDenominator + previousDenominator) > 1000) && (currentNumerator < (currentCMA * 2.7)) )  ) //First 1000 values are learning phase(Burn in phase) to converge to a decent CMA
                {
	              	a = currentNumerator + previousNumerator
			b = currentDenominator + previousDenominator
			c = 0
             	}
		if ( ((currentDenominator + previousDenominator) > 1000) && (currentNumerator >= (currentCMA * 2.7)) ) 	//Checking for seizure
                {
                   	a = previousNumerator
                        b = previousDenominator
                        c = currentAnomalyCounter + previousAnomalyCounter
                }
		(a,b,c)
	}


        //Setting system properties
	val conf = new SparkConf()
	.setMaster("spark://hostname:7077")
	.setAppName("StreamAnomalyDetector")
	.setSparkHome(System.getenv("SPARK_HOME"))
	.setJars(List("target/scalaad-1.0-SNAPSHOT-jar-with-dependencies.jar"))
	.set("spark.executor.memory", "7g")
        .set("spark.cores.max","36")
        .set("spark.cleaner.ttl","300")
        .set("spark.default.parallelism","4")
        .set("spark.executor.logs.rolling.strategy", "size") 
        .set("spark.executor.logs.rolling.maxSize", "2048") 
        .set("spark.executor.logs.rolling.maxRetainedFiles", "6")
        .set("spark.streaming.receiver.maxRate","175")
        .set("spark.streaming.unpersist","true")
        .set("spark.streaming.blockInterval","200")
        .set("spark.streaming.concurrentJobs","10")
        .set("spark.worker.cleanup.enabled" , "true")
	.set("spark.worker.cleanup.interval","3600")
	//.set("spark.streaming.receiver.writeAheadLog.enable","true")
        //.set("spark.localExecution.enabled","true")
        //.set("spark.speculation","true")
        //.set("spark.locality.wait","3000")        
        //.set("spark.shuffle.consolidateFiles","true")
        //.set("spark.shuffle.memoryFraction","0.2")¨

        val zkQuorum = "ip1:2181,ip2:2181,ip3:2181,ip4:2181,ip5:2181"
        val group = "test-group"
    
        // Create the context
	val ssc = new StreamingContext(conf, Seconds(3))
        	
        //hdfs path to checkpoint old data
        ssc.checkpoint("hdfs://host-10-1-4-90.novalocal:9000/user/hduser/checkpointing")
	val joinedResult = new Array[DStream[(Long, (Int, (Long, String)))]](12)	

	// Create a KafkaDStream
        val eegStream = KafkaUtils.createStream(ssc, zkQuorum, group, Map(args(0) -> 1),StorageLevel.MEMORY_ONLY).map(_._2)
	val windowedEEG = eegStream.window(Seconds(3), Seconds(3)).cache()	
	
	val timeAndFile = windowedEEG.map(x=> { val token = x.split(",")
                                                (math.round(token(1).toDouble),token(0))
                                                })
     	
	val firstTimeAndFile = timeAndFile.transform( rdd => rdd.context.makeRDD(rdd.top(1))).map(x=>(1L,(x._1,x._2)))
	ssc.sparkContext.parallelize((2 to 24).map(i=> {
						val counts = windowedEEG.map(x=> { val token = x.split(",")
                                                (math.abs(math.round(token(i).toDouble)))
                                                }).countByValue()



	val topCounts = counts.map(_.swap).transform( rdd => rdd.context.makeRDD(rdd.top(60)))        

        val numer_denom = topCounts.map(x => (x._2 * x._1 , x._1 )).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

        val amplitudeTopK  = numer_denom.map(x => (x._1.toFloat/x._2))                              //Amplitude of Top-K for Normal Data  
	
        val CMA = amplitudeTopK.map(r => (1,(r.toDouble,1,1))).updateStateByKey[(Double,Int,Int)](updateSum).map(_._2)            
        val anomaly = CMA.map(x => (1L , x._3))
        joinedResult(i) = anomaly.join(firstTimeAndFile)

}))	

	var unionedResult =  joinedResult(2)
	for (a <- 3 to 11)
	{
		unionedResult = unionedResult.union(joinedResult(a))      
	}

	unionedResult.map(x => "%s,%s,%s".format(x._2._2._2, x._2._2._1, x._2._1)).repartition(1).saveAsTextFiles("hdfs://host-10-1-4-90.novalocal:9000/user/hduser/output/")
	unionedResult.print()

    ssc.start()
    ssc.awaitTermination()
    }
}

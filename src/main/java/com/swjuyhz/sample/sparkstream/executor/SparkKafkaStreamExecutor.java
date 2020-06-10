package com.swjuyhz.sample.sparkstream.executor;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import shapeless.record;

import java.io.Serializable;
import java.util.*;

@Component
public class SparkKafkaStreamExecutor implements Serializable,Runnable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(SparkKafkaStreamExecutor.class);

	static final String path = "/home/jojang/dev/workspace/spark/data/";

	@Value("${spark.stream.kafka.durations}")
	private String streamDurationTime;
	@Value("${kafka.broker.list}")
	private String metadatabrokerlist;
	@Value("${spark.kafka.topics}")
	private String topicsAll;
	@Autowired
	private transient Gson gson;

	private transient JavaStreamingContext jsc;
	@Autowired 
	private transient JavaSparkContext javaSparkContext;

	@Autowired
	private SparkSession sparkSession;
	
	@Override
	public void run() {
		startStreamTask();
	}
	
	public void startStreamTask() {
		// System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.5");
		Collection<String> topics = Arrays.asList("topic");

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", metadatabrokerlist);
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);
		
		jsc = new JavaStreamingContext(javaSparkContext,
				Durations.seconds(Integer.valueOf(streamDurationTime)));
		jsc.checkpoint("checkpoint"); //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复

		// 得到数据流
//		final JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(jsc, f,
//				String.class, StringDeserializer.class, StringDeserializer.class, kafkaParams, topics);

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						jsc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe( topics, kafkaParams)
				);



		System.out.println("stream started!");
		stream.print();
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			System.out.println( "rdd.count:" + rdd.count() );
		});
		stream.foreachRDD( x -> {
			x.collect().forEach( (xx)-> { System.out.println("Kafka Received Data:" + xx.value());  });

			x.collect().forEach( (xx)-> {
				System.out.println("Kafka Received Data:" + xx.value());
				List<KafkaData> data = Arrays.asList(new KafkaData(null, xx.value()));
				Dataset<Row> dataFrame = sparkSession.createDataFrame(data, KafkaData.class);
				dataFrame.createOrReplaceTempView("my_kafka");
				Dataset<Row> sqlDS=sparkSession.sql("select * from my_kafka");
				sqlDS.printSchema();
				sqlDS.show();
			});

		});

		stream.foreachRDD(x -> {
			if (x.count() > 0) {
				x.collect().forEach((xx) -> {
					System.out.println("Kafka Received Data:" + xx.value());
				});

				List<KafkaData> data = new ArrayList<>();
				x.collect().forEach(record -> {
							data.add(new KafkaData(null, record.value()));
						}
				);

				Dataset<Row> dataFrame = sparkSession.createDataFrame(data, KafkaData.class);
				dataFrame.createOrReplaceTempView("my_kafka2");
				Dataset<Row> sqlDS = sparkSession.sql("select * from my_kafka2");
				sqlDS.printSchema();
				sqlDS.show();


				dataFrame.write().mode(SaveMode.Append).parquet(path + "my_kafka2.parquet");
			}
		});


//		stream.foreachRDD ( rdd -> {
//					rdd.foreach(record -> {
//								String value = record.value();
//								System.out.println("vvv:" + value);
//							}
//					);
//					}
//		);


//		stream.foreachRDD(v -> {
////			//针对单篇文章流式处理
////			List<String> topicDatas = v.map(x-> (String)x.value()).collect();//优化点：不收集而是并发节点处理？
////			for (String topicData : topicDatas) {
////				List<Map<String, Object>> list = gson
////						.fromJson(topicData, new TypeToken<List<Map<String, String>>>() {}.getType());
////				list.parallelStream().forEach(m->{
////					//do something
////					System.out.println(m);
////				});
////			}
////			log.info("一批次数据流处理完： {}",topicDatas);
//		});

//		stream.foreachRDD(rdd -> {
//			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//			System.out.println( "rdd.count:" + rdd.count() );
////			rdd.take(1).forEach(x -> System.out.println("value::::" + x.value()) );
//			for (OffsetRange offsetRange : offsetRanges) {
//				System.out.println("fromOffset:"+ offsetRange.fromOffset() );
//			}
//		});



		;
		//优化：为每个分区创建一个连接
//		stream.foreachRDD(t->{
//			t.foreachPartition(f->{
//				while(f.hasNext()) {
//					Map<String, Object> symbolLDAHandlered =LDAModelPpl
//							.LDAHandlerOneArticle(sparkSession, SymbolAndNews.symbolHandlerOneArticle(sparkSession, f.next()._2));
//				}
//			});
//		});
		jsc.start();
	}

	public void destoryStreamTask() {
		if(jsc!=null) {
			jsc.stop();
		}		
	}

}

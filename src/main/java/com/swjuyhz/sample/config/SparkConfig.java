package com.swjuyhz.sample.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

@Configuration
public class SparkConfig {
	@Value("${spring.application.name}")
	private String sparkAppName;
	@Value("${spark.master}")
	private String sparkMasteer;
	@Value("${spark.stream.kafka.durations}")
	private String streamDurationTime;
	@Value("${spark.driver.memory}")
	private String sparkDriverMemory;
	@Value("${spark.worker.memory}")
	private String sparkWorkerMemory;
	@Value("${spark.executor.memory}")
	private String sparkExecutorMemory;
	@Value("${spark.rpc.message.maxSize}")
	private String sparkRpcMessageMaxSize;

	@Bean
//	@ConditionalOnMissingBean(SparkConf.class)
	public SparkConf sparkConf() {
		SparkConf conf = new SparkConf()
				.setAppName(sparkAppName)
                .setMaster(sparkMasteer).set("spark.driver.memory",sparkDriverMemory)
                .set("spark.worker.memory",sparkWorkerMemory)//"26g".set("spark.shuffle.memoryFraction","0") //默认0.2
                .set("spark.executor.memory",sparkExecutorMemory)
                .set("spark.rpc.message.maxSize",sparkRpcMessageMaxSize)
				.set("spark.serializer","org.apache.spark.serializer.KryoSerialize")
				.registerKryoClasses((Class<ConsumerRecord>[] ) Arrays.asList(ConsumerRecord.class).toArray());
		// .setMaster("local[*]");//just use in test
		return conf;
	}


	@Bean
//	@ConditionalOnMissingBean(JavaSparkContext.class)
	public JavaSparkContext javaSparkContext() {
		return new JavaSparkContext(sparkConf());
	}

	@Bean
	public SparkSession sparkSession() {
		return SparkSession
				.builder()
				.sparkContext(javaSparkContext().sc())
				.appName("Java Spark SQL basic example")
				.getOrCreate();
	}




}

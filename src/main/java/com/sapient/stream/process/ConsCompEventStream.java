package com.sapient.stream.process;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import com.google.common.base.Optional;
import com.google.common.io.Files;

import scala.Tuple2;

/**
 * A spark streaming processor to ingest consumer complaints events from a flume
 * agent, calculate a running count per product and state, and periodically
 * saving the counts to text files.
 * 
 * @author abhinavg6
 *
 */
public class ConsCompEventStream {

	// Create the context with checkpointing
	private static JavaStreamingContext createContext(String host, int port,
			String checkpointDir, final String outputPath) {

		SparkConf sparkConf = new SparkConf().setAppName("ConsCompEventStream");
		// Uncomment for running via IDE locally
		// SparkConf sparkConf = new
		// SparkConf().setAppName("ConsCompEventStream").setMaster("local[*]");

		// Create the context and set the checkpoint directory
		JavaStreamingContext context = new JavaStreamingContext(sparkConf,
				new Duration(2000));
		context.checkpoint(checkpointDir);

		// Create the input stream from flume sink
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils
				.createStream(context, host, port);

		// Transform each flume avro event to a process-able format
		JavaDStream<String> transformedEvents = flumeStream
				.map(new Function<SparkFlumeEvent, String>() {

					@Override
					public String call(SparkFlumeEvent flumeEvent)
							throws Exception {
						String flumeEventStr = flumeEvent.event().toString();
						String transformedEvent = flumeEventStr.substring(
								flumeEventStr.indexOf("[") + 1,
								flumeEventStr.indexOf("]"));
						return transformedEvent;
					}
				});

		// Map key-value pairs by product and state to count of 1
		JavaPairDStream<Tuple2<String, String>, Integer> countOnes = transformedEvents
				.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {

					@Override
					public Tuple2<Tuple2<String, String>, Integer> call(
							String transformedEvent) throws Exception {

						return new Tuple2<Tuple2<String, String>, Integer>(
								extractKeyTuple(transformedEvent), 1);
					}

				});

		// Maintain a updated list of counts per product and state
		JavaPairDStream<Tuple2<String, String>, List<Integer>> updatedCounts = countOnes
				.updateStateByKey(new Function2<List<Integer>, Optional<List<Integer>>, Optional<List<Integer>>>() {

					@Override
					public Optional<List<Integer>> call(List<Integer> values,
							Optional<List<Integer>> state) throws Exception {
						// Get the existing state
						List<Integer> stateList = state
								.or(new ArrayList<Integer>());

						// Get the total count from current batch, and use it
						// and existing state to create new state
						int newTotalCount = values.size();
						List<Integer> newStateList = new ArrayList<Integer>();
						newStateList.addAll(stateList);
						newStateList.add(newTotalCount);

						return Optional.of(newStateList);
					}
				});

		// Store the updated list of counts in a text file per product and state
		updatedCounts
				.foreachRDD(new Function<JavaPairRDD<Tuple2<String, String>, List<Integer>>, Void>() {

					@Override
					public Void call(
							JavaPairRDD<Tuple2<String, String>, List<Integer>> countRDD)
							throws Exception {
						List<Tuple2<Tuple2<String, String>, List<Integer>>> rddList = countRDD
								.collect();
						for (Tuple2<Tuple2<String, String>, List<Integer>> rddTuple : rddList) {
							Tuple2<String, String> keyTuple = rddTuple._1;
							List<Integer> valueTuple = rddTuple._2;
							File outputFile = new File(outputPath
									+ keyTuple._1.replaceAll("\\s", "_") + "_"
									+ keyTuple._2 + ".dat");
							Files.append(valueTuple.toString() + "\n",
									outputFile, Charset.defaultCharset());
						}
						return null;
					}
				});

		return context;
	}

	// Extract the key tuple out of transformed event
	private static Tuple2<String, String> extractKeyTuple(String transEvent) {
		String[] transEventVals = transEvent.split(",");

		// Create the key of product and state
		String product = transEventVals[0].trim();
		product = product.isEmpty() ? "UNKNOWN" : product;
		String state = transEventVals[transEventVals.length - 1].trim();
		state = state.isEmpty() ? "UNKNOWN" : state;
		Tuple2<String, String> keyTuple = new Tuple2<String, String>(product,
				state);

		return keyTuple;
	}

	public static void main(String[] args) {

		final String checkpointDir = args[0]; // Pass a local dir, for e.g.
												// /usr/local/etc/spark-checkpoint
		final String outputDir = args[1]; // Pass local dir, for e.g.
											// /usr/local/etc/spark-output/
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {

			@Override
			public JavaStreamingContext create() {
				return createContext("localhost", 43333, checkpointDir,
						outputDir);
			}
		};

		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(
				checkpointDir, factory);
		ssc.start();
		ssc.awaitTermination();
	}
}

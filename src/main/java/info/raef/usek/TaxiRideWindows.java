package info.raef.usek;

import com.dataartisans.flinktraining.examples.datastream_java.connectors.PopularPlacesFromKafka;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import javax.xml.crypto.Data;
import java.util.*;

public class TaxiRideWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // *******************************
        // ******************************* FROM Kafka
        // *******************************
//        Properties props = new Properties();
//        props.setProperty("zookeeper.connect", "localhost:2181");
//        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("group.id", "UsekGroup");
//        props.setProperty("auto.offset.reset", "earliest");
//
//        FlinkKafkaConsumer<TaxiRide> consumer =
//                new FlinkKafkaConsumer<TaxiRide>(
//                        "cleansedRides",
//                        new TaxiRideSchema(),
//                        props
//                );
//        consumer.assignTimestampsAndWatermarks(new PopularPlacesFromKafka.TaxiRideTSExtractor());
//        DataStream<TaxiRide> rides = env.addSource(consumer);

        // *******************************
        // *******************************
        // *******************************


        final int maxEventDelay = 60;
        final int servingSpeedFactor = 100;
        final int popThreshold = 20;
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", maxEventDelay, servingSpeedFactor)
        );

        DataStream<TaxiRide> filteredRides = rides.filter((FilterFunction<TaxiRide>) (TaxiRide ride) -> GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat));
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularSpots = filteredRides
                .map(new MapFunction<TaxiRide, Tuple2<Integer, Boolean>>() {
                    @Override
                    public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
                        if (taxiRide.isStart) {
                            int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                            return new Tuple2<>(gridId, true);
                        } else {
                            int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                            return new Tuple2<>(gridId, false);
                        }
                    }
                })
                .<KeyedStream<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>>keyBy(0, 1)
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new WindowFunction<Tuple2<Integer, Boolean>,
                        Tuple4<Integer, Long, Boolean, Integer>,
                        Tuple,
                        TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple2<Integer, Boolean>> values, Collector<Tuple4<Integer, Long, Boolean, Integer>> collector) throws Exception {
                        int cellId = ((Tuple2<Integer, Boolean>) key).f0;
                        boolean isStart = ((Tuple2<Integer, Boolean>) key).f1;
                        long windowTime = timeWindow.getEnd();
                        int count = 0;
                        for (Tuple2<Integer, Boolean> v : values) {
                            count += 1;
                        }
                        collector.collect(new Tuple4<>(cellId, windowTime, isStart, count));
                    }
                })
                .filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
                        return count.f3 >= popThreshold;
                    }
                })
                .map(new MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>>() {
                    @Override
                    public Tuple5<Float, Float, Long, Boolean, Integer> map(Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {
                        return new Tuple5<>(
                                GeoUtils.getGridCellCenterLon(cellCount.f0),
                                GeoUtils.getGridCellCenterLat(cellCount.f0),
                                cellCount.f1,
                                cellCount.f2,
                                cellCount.f3);
                    }
                });
//        popularSpots.print();

        // *******************************
        // *******************************
        // ******************************* TO ELASTICSEARCH
        // *******************************
        // *******************************
        // *******************************

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));

        // use an ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple5<Float, Float, Long, Boolean, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple5<Float, Float, Long, Boolean, Integer>>() {

                    public IndexRequest createIndexRequest(Tuple5<Float, Float, Long, Boolean, Integer> record) {
                        Map<String, String> json = new HashMap<>();
                        json.put("time", record.f2.toString());         // timestamp
                        json.put("location", record.f1+","+record.f0);  // lat,lon pair
                        json.put("isStart", record.f3.toString());      // isStart
                        json.put("cnt", record.f4.toString());          // count

                        return Requests.indexRequest()
                                .index("nyc-places")           // index name
                                .type("popular-locations")     // mapping name
                                .source(json);
                    }

                    @Override
                    public void process(Tuple5<Float, Float, Long, Boolean, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(...)
//                    restClientBuilder.setMaxRetryTimeoutMillis(...)
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );

        // finally, build and add the sink to the job's pipeline
        popularSpots.addSink(esSinkBuilder.build());

        // *******************************
        // *******************************
        // *******************************
        // *******************************
        // *******************************
        // *******************************

        env.execute();
    }
}
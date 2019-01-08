package info.raef.usek;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class TaxiRideCleansing {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", maxEventDelay, servingSpeedFactor)
        );
        DataStream<TaxiRide> filteredRides = rides.filter((FilterFunction<TaxiRide>) (TaxiRide ride) -> GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat));
        filteredRides.print();


        // ****************************
        // ****************************
        // Write to Kafka ****************************

//        filteredRides.addSink(
//          new FlinkKafkaProducer<TaxiRide>(
//                  "localhost:9092",
//                  "cleansedRides",
//                  new TaxiRideSchema()
//          )
//        );
        env.execute();
    }
}
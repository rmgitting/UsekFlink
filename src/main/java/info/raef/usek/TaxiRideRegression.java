package info.raef.usek;

import com.dataartisans.flinktraining.examples.datastream_java.state.TravelTimePrediction;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class TaxiRideRegression {
    public static void main(String[] args) throws Exception {
        final int servingSpeedFactor = 100;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                20,
                Time.of(10, TimeUnit.SECONDS)
        ));
        DataStream<TaxiRide> rides = env.addSource(new CheckpointedTaxiRideSource("D:\\Projects\\Java\\UsekFlink\\nycTaxiRides.gz", servingSpeedFactor));

        DataStream<Tuple2<Long, Integer>> predictions =
                rides.filter((FilterFunction<TaxiRide>) (TaxiRide ride) -> GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat))
                        .map(new TravelTimePrediction.GridCellMatcher())
                        .keyBy(0)
                        .flatMap(new RichFlatMapFunction<Tuple2<Integer, TaxiRide>, Tuple2<Long, Integer>>() {
                            private transient ValueState<TravelTimePredictionModel> modelState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<TravelTimePredictionModel> descriptor =
                                        new ValueStateDescriptor<TravelTimePredictionModel>(
                                                "regressionModel",
                                                TypeInformation.of(new TypeHint<TravelTimePredictionModel>() {}),
                                                new TravelTimePredictionModel()
                                        );
                                modelState = getRuntimeContext().getState(descriptor);
                            }

                            @Override
                            public void flatMap(Tuple2<Integer, TaxiRide> val, Collector<Tuple2<Long, Integer>> out) throws Exception {
                                TravelTimePredictionModel model = modelState.value();
                                TaxiRide ride = val.f1;
                                double distance = GeoUtils.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat);
                                int direction = GeoUtils.getDirectionAngle(ride.endLon, ride.endLat, ride.startLon, ride.startLat);
                                if(ride.isStart) {
                                    int predictedTime = model.predictTravelTime(direction, distance);
                                    out.collect(new Tuple2<>(ride.rideId, predictedTime));

                                } else {
                                    double travelTime = (ride.endTime.getMillis() - ride.startTime.getMillis()) / 60000.0;
                                    model.refineModel(direction, distance, travelTime);
                                    modelState.update(model);
                                }

                            }
                        });
        predictions.print();
        env.execute();
    }
}
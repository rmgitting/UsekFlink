package info.raef.usek;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.Map;
import java.util.Random;

public class TaxiRideQuerySystem {

    private static class Query {
        private final long queryId;
        private final float longitude;
        private final float latitude;

        public Query(float longitude, float latitude) {
            this.queryId = new Random().nextLong();
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public long getQueryId() {
            return queryId;
        }

        public float getLongitude() {
            return longitude;
        }

        public float getLatitude() {
            return latitude;
        }

        @Override
        public String toString() {
            return "Query{" +
                    "id=" + queryId +
                    ", longitude=" + longitude +
                    ", latitude=" + latitude +
                    '}';
        }
    }

    final static MapStateDescriptor queryDescriptor = new MapStateDescriptor<>(
            "queries",
            BasicTypeInfo.LONG_TYPE_INFO,
            TypeInformation.of(Query.class)
    );
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("nycTaxiRides.gz", maxEventDelay, servingSpeedFactor));
        BroadcastStream<Query> queryStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Query>() {
                    @Override
                    public Query map(String s) throws Exception {
                        String[] parts = s.split(",\\s*");
                        return new Query(Float.valueOf(parts[0]), Float.valueOf(parts[1]));
                    }
                })
                .broadcast(queryDescriptor);

        DataStream<Tuple3<Long, Long, Float>> reports = rides
                .keyBy((TaxiRide ride) -> ride.taxiId)
                .connect(queryStream)
                .process(new KeyedBroadcastProcessFunction<Long, TaxiRide, Query, Tuple3<Long, Long, Float>>() {
                    @Override
                    public void processElement(TaxiRide taxiRide, ReadOnlyContext readOnlyContext, Collector<Tuple3<Long, Long, Float>> collector) throws Exception {
                        if(!taxiRide.isStart) {
                            Iterable<Map.Entry<Long, Query>> entries = readOnlyContext.getBroadcastState(queryDescriptor).immutableEntries();

                            for(Map.Entry<Long, Query> entry: entries) {
                                final Query query = entry.getValue();
                                final float kilometersAway = (float) taxiRide.getEuclideanDistance(query.getLongitude(), query.getLatitude());
                                collector.collect(new Tuple3<>(query.getQueryId(),
                                        taxiRide.taxiId,
                                        kilometersAway));
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(Query query, Context context, Collector<Tuple3<Long, Long, Float>> collector) throws Exception {
                        System.out.println("new query " + query);
                        context.getBroadcastState(queryDescriptor).put(query.getQueryId(), query);

                    }
                });

        DataStream<Tuple3<Long, Long, Float>> nearest = reports
                .keyBy((Tuple3<Long, Long, Float> event) -> event.f0)
                .process(new KeyedProcessFunction<Long, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>>() {
                    private transient ValueState<Tuple2<Long, Float>> closest;
                    @Override
                    public void processElement(Tuple3<Long, Long, Float> report, Context context, Collector<Tuple3<Long, Long, Float>> collector) throws Exception {
                        if (closest.value() == null || report.f2 <closest.value().f1) {
                            closest.update(new Tuple2<>(report.f1, report.f2));
                            collector.collect(report);
                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Tuple2<Long, Float>> descriptor =
                                new ValueStateDescriptor<Tuple2<Long, Float>>(
                                        "report",
                                        TypeInformation.of(new TypeHint<Tuple2<Long, Float>>() {}));
                        closest = getRuntimeContext().getState(descriptor);
                    }

                });
        nearest.print();
        env.execute();
    }
}
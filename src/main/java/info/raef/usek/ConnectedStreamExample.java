package info.raef.usek;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class ConnectedStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> control = env.fromElements("DROP", "IGNORE");
        DataStream<String> data = env.fromElements("data", "DROP", "cognitus", "IGNORE");

        DataStream<String> result = control.connect(data).flatMap(new CoFlatMapFunction<String, String, String>() {
            HashSet controls = new HashSet();

            @Override
            public void flatMap1(String s, Collector<String> collector) throws Exception {
                controls.add(s);
                collector.collect("listed control: " + s);
            }

            @Override
            public void flatMap2(String s, Collector<String> collector) throws Exception {
                if (controls.contains(s))
                    collector.collect("controlled: " + s);
                else
                    collector.collect("accepted " + s);

            }
        });

        result.print();
        env.execute();
    }
}

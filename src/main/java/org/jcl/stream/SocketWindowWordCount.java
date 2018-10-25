package org.jcl.stream;/**
 * Created by admin on 2018/9/13.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @author jichenglu
 * @create 2018-09-13 13:10
 **/
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception{
//        cdata();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.readTextFile("data/1.txt");

        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        for (String word : value.split(" ")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
//                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    public static void cdata(){
        Jedis jedis=new Jedis("app",6379);
        jedis.auth("touchspring");
        jedis.select(0);

        Set<String> keys=jedis.keys("imei:TEST00*");
        for(String key:keys){
            jedis.del(key);
        }

        jedis.close();
    }
}

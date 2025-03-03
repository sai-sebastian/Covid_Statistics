package hadoop.covid_statistics;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.bson.BSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class MongoCovidStatisticsMap extends Mapper<Object, BSONObject, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text city = new Text();

    @Override
    protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        String val = (String) value.get("city");
        city.set(val);
        context.write(city, one);
    }
}
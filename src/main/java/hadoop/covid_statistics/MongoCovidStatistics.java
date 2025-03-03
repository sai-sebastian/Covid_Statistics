package hadoop.covid_statistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

import hadoop.covid_statistics.MongoCovidStatistics;

public class MongoCovidStatistics extends MongoTool {
    public MongoCovidStatistics() {
        Configuration conf = new Configuration();
        MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
        MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
        MongoConfigUtil.setInputURI(conf, "mongodb://root:example@mongodb:27017/Covid_Statistics.Patient?authSource=admin");
        MongoConfigUtil.setQuery(conf, "{}");
        MongoConfigUtil.setOutputURI(conf, "mongodb://root:example@mongodb:27017/Covid_Statistics.Statistics?authSource=admin");
        MongoConfigUtil.setMapper(conf, MongoCovidStatisticsMap.class);
        MongoConfigUtil.setReducer(conf, MongoCovidStatisticsReduce.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, IntWritable.class);
        MongoConfigUtil.setOutputKey(conf, ObjectId.class);
        MongoConfigUtil.setOutputValue(conf, BSONObject.class);
        setConf(conf);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MongoCovidStatistics(), args));
    }
}


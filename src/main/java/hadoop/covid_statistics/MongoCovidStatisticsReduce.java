package hadoop.covid_statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bson.BSONObject;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;

public class MongoCovidStatisticsReduce extends Reducer<Text, IntWritable, ObjectId, BSONWritable> {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> statisticsCollection;
    private MongoCollection<Document> mostAffectedCityCollection;
    private Map<String, Integer> cityPatientCountMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // MongoDB connection URI with authentication
        MongoClientURI uri = new MongoClientURI("mongodb://root:example@mongodb:27017/?authSource=admin");
        mongoClient = new MongoClient(uri);

        // Initialize the database and collections
        database = mongoClient.getDatabase("Covid_Statistics");
        statisticsCollection = database.getCollection("Statistics");
        mostAffectedCityCollection = database.getCollection("Most_Affected_City");

        // Initialize city patient count map
        cityPatientCountMap = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Store the number of patients by city in the statistics collection
        BSONObject doc = BasicDBObjectBuilder.start()
                .add("city", key.toString())
                .add("number_of_patients", sum)
                .get();
        statisticsCollection.insertOne(new Document(doc.toMap()));

        // Store the count in a map for determining the most affected city
        cityPatientCountMap.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Determine the most affected city
        String mostAffectedCity = null;
        int maxPatients = 0;
        for (Map.Entry<String, Integer> entry : cityPatientCountMap.entrySet()) {
            if (entry.getValue() > maxPatients) {
                maxPatients = entry.getValue();
                mostAffectedCity = entry.getKey();
            }
        }

        // Store the most affected city in the most_affected_city collection
        if (mostAffectedCity != null) {
            Document mostAffectedCityDoc = new Document("city", mostAffectedCity)
                    .append("number_of_patients", maxPatients);
            mostAffectedCityCollection.drop(); // Optional: Clear the collection before inserting
            mostAffectedCityCollection.insertOne(mostAffectedCityDoc);
        }

        // Close the MongoDB client
        mongoClient.close();
    }
}
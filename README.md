# KafkaStreams_TopTen
Here we are joining the messages in these two topics(users & pageviews) on the user id field by using a 1 minute hopping window with 10 second advances to compute the 10 most viewed pages by viewtime for every value of gender. Once per minute produces a message into the top_pages topic that contains the gender, page id, sum of view time in the latest window and distinct count of user ids in the latest window

# Use maven to build:
    1. pom.xml contains all dependancies

    2. download this repository to local empty directory

    3. run "mvn package" to build

These steps will create a jar file/files of Kafka streaming top ten project which we can use to run.

# How to run?
1. Start the Confluent quickstart docker images
2. Create topics (users/pageviews) as  per quickstart guide in Kafka

2. Use the following command to run the Kafka Streaming job
```
java -jar kafka-streams-top10-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.abhijeet.kafkastreams.toppages.TopPageViews
```
    



package com.abhijeet.kafkastreams.toppages;

import java.io.IOException;

import java.io.InputStream;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueStore;

import com.abhijeet.kafkastreams.toppages.utils.PriorityQueueSerde;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

class UserPageViewConstants {
	public static final String APPLICATION_ID_CONFIG = "top10pageviews";
	public static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	public static final String AUTO_OFFSET_RESET_CONFIG = "earliest";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String USER_PAGEVIEW_SCHEMA_FILE = "users_pageviews.avsc";
	public static final String TOP_PAGES_SCHEMA_FILE = "toppages.avsc";
	public static final String GENDER = "gender";
	public static final String PAGE_ID = "pageid";
	public static final String TOTAL_VIEW_TIME = "total_viewtime";
	public static final String UNIQUE_USERS = "unique_users";
	public static final String USER_ID = "userid";
	public static final String VIEW_TIME = "viewtime";
	public static final String PAGEVIEWS = "pageviews";
	public static final String USERS = "users";
}

public class TopPageViews {

	private static Serde<String> keySerde;
	private static Serde<GenericRecord> valueSerde;
	private final static Serde<Windowed<String>> windowedStringSerde = WindowedSerdes
			.timeWindowedSerdeFrom(String.class);

	/**
	 * @param args
	 * @throws IOException
	 */
	public TopPageViews(Serde<String> keySerde, Serde<GenericRecord> valueSerde) {
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;

	}

	private Properties loadProperties() {
		final Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, UserPageViewConstants.APPLICATION_ID_CONFIG);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, UserPageViewConstants.BOOTSTRAP_SERVERS_CONFIG);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, UserPageViewConstants.AUTO_OFFSET_RESET_CONFIG);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		final String schemaRegistryUrl = UserPageViewConstants.SCHEMA_REGISTRY_URL;
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
		return props;
	}



	public static void main(String[] args) throws IOException {
		final Serde<String> keySerde = Serdes.String();
		final String schemaRegistryUrl = UserPageViewConstants.SCHEMA_REGISTRY_URL;
		final StreamsBuilder builder = new StreamsBuilder();
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		final GenericAvroSerde genericAvroSerde = getAvroSerde();

		TopPageViews topPageViews = new TopPageViews(keySerde, genericAvroSerde);
		Properties props = topPageViews.loadProperties();

		// Read messages from pageviews topic as a stream
		final KStream<String, GenericRecord> viewsByUserStream = builder.stream(UserPageViewConstants.PAGEVIEWS);

		// Read messages from users topic as a GlobalKTable
		final GlobalKTable<String, GenericRecord> usersTable = builder.globalTable(UserPageViewConstants.USERS,
				Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("users-store")
						.withKeySerde(keySerde).withValueSerde(valueSerde));

		final KStream<String, GenericRecord> usersPageViews = topPageViews.joinUsersWithPageviews(viewsByUserStream,
				usersTable);

		final KTable<Windowed<String>, GenericRecord> pageViewsStats = topPageViews
				.aggregateUsersByGender(usersPageViews);

		final KStream<String, GenericRecord> topPagesStream = topPageViews.topTenPagesByGender(pageViewsStats);

		topPagesStream.to("top_pages");
		topPagesStream.print(Printed.toSysOut());

		final KafkaStreams streams = new KafkaStreams(builder.build(), props);
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (final Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
	
	private KStream<String, GenericRecord> joinUsersWithPageviews(KStream<String, GenericRecord> viewsByUserStream,
			GlobalKTable<String, GenericRecord> usersTable) {

		final Schema schema = loadSchema(UserPageViewConstants.USER_PAGEVIEW_SCHEMA_FILE);

		return viewsByUserStream.join(usersTable,
				(pageViewId, pageView) -> pageView.get(UserPageViewConstants.USER_ID) != null
						? pageView.get(UserPageViewConstants.USER_ID).toString()
						: "",
				(pageView, user) -> {
					final GenericRecord record = new GenericData.Record(schema);
					record.put(UserPageViewConstants.GENDER, user.get(UserPageViewConstants.GENDER));
					record.put(UserPageViewConstants.PAGE_ID, pageView.get(UserPageViewConstants.PAGE_ID));
					record.put(UserPageViewConstants.VIEW_TIME, pageView.get(UserPageViewConstants.VIEW_TIME));
					record.put(UserPageViewConstants.USER_ID, user.get(UserPageViewConstants.USER_ID));
					return record;
				});

	}

	private static GenericAvroSerde getAvroSerde() {
		final GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
		genericAvroSerde.configure(schemaRegistryConfig(UserPageViewConstants.SCHEMA_REGISTRY_URL), false);
		return genericAvroSerde;
	}
	
	
	private KStream<String, GenericRecord> topTenPagesByGender(KTable<Windowed<String>, GenericRecord> pageViewsStats) {

		final Schema topPageSchema = loadSchema(UserPageViewConstants.TOP_PAGES_SCHEMA_FILE);
		final Comparator<GenericRecord> comparator = (o1,
				o2) -> (int) ((Long) o2.get(UserPageViewConstants.TOTAL_VIEW_TIME)
						- (Long) o1.get(UserPageViewConstants.TOTAL_VIEW_TIME));

		DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault());

		final GenericAvroSerde genericAvroSerde = getAvroSerde();
		final KStream<String, GenericRecord> topPagesStream = pageViewsStats.groupBy((windowedKey, aggPageView) -> {
			final Windowed<String> windowedGenderKey = new Windowed<>(windowedKey.key().split(":")[0],
					windowedKey.window());

			final GenericRecord topPage = new GenericData.Record(topPageSchema);
			topPage.put(UserPageViewConstants.GENDER, aggPageView.get(UserPageViewConstants.GENDER));
			topPage.put(UserPageViewConstants.PAGE_ID, aggPageView.get(UserPageViewConstants.PAGE_ID));
			topPage.put(UserPageViewConstants.TOTAL_VIEW_TIME, aggPageView.get(UserPageViewConstants.TOTAL_VIEW_TIME));
			topPage.put(UserPageViewConstants.UNIQUE_USERS,
					(long) ((Map<String, Boolean>) aggPageView.get("users")).size());

			return new KeyValue<>(windowedGenderKey, topPage);
		}, Grouped.with(windowedStringSerde, valueSerde))
				.aggregate(() -> new PriorityQueue<>(comparator), (windowedGenderKey, aggPageView, queue) -> {
					queue.add(aggPageView);
					return queue;
				}, (windowedGenderKey, aggPageView, queue) -> {
					queue.remove(aggPageView);
					return queue;
				}, Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, genericAvroSerde)))
				.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(60), Suppressed.BufferConfig.unbounded()))
				.toStream().flatMap((key, queue) -> {
					if (queue == null) {
						return Collections.emptyList();
					}

					return IntStream.range(0, 10).mapToObj(i -> queue.poll()).filter(Objects::nonNull)
							.map(topPage -> new KeyValue<>(
									String.format("%s:%s", formatter.format(key.window().startTime()), key.key()),
									topPage))
							.collect(Collectors.toList());

				});

		return topPagesStream;
	}

	private KTable<Windowed<String>, GenericRecord> aggregateUsersByGender(
			KStream<String, GenericRecord> usersPageViews) {
		final Schema aggregatedSchema = loadSchema("aggregated.avsc");

		final KTable<Windowed<String>, GenericRecord> pageViewsStats = usersPageViews
				.groupBy((userId, pageView) -> String.format("%s:%s", pageView.get("gender"), pageView.get("pageid")))
				.windowedBy(TimeWindows.of(Duration.ofSeconds(60)).advanceBy(Duration.ofSeconds(10))).aggregate(() -> {
					GenericRecord record = null;
					try {
						record = new GenericData.Record(aggregatedSchema);
						record.put(UserPageViewConstants.GENDER, "");
						record.put(UserPageViewConstants.PAGE_ID, "");
						record.put(UserPageViewConstants.TOTAL_VIEW_TIME, 0L);
						record.put(UserPageViewConstants.USERS, new HashMap<>());
					} catch (Exception e) {
						e.printStackTrace();
					}
					return record;
				}, (key, pageView, agg) -> {
					try {
						agg.put(UserPageViewConstants.GENDER, pageView.get(UserPageViewConstants.GENDER));
						agg.put(UserPageViewConstants.PAGE_ID, pageView.get(UserPageViewConstants.PAGE_ID).toString());
						agg.put(UserPageViewConstants.TOTAL_VIEW_TIME,
								(Long) agg.get(UserPageViewConstants.TOTAL_VIEW_TIME)
										+ (Long) pageView.get(UserPageViewConstants.VIEW_TIME));
						((Map<String, Boolean>) agg.get(UserPageViewConstants.USERS))
								.put(pageView.get(UserPageViewConstants.USER_ID).toString(), true);
					} catch (Exception e) {
						e.printStackTrace();
					}
					return agg;
				}, Materialized.as("agg-page_views"));

		return pageViewsStats;
	}

	private Schema loadSchema(String schemaFileName) {
		Schema schema = null;

		try {
			final InputStream schemaFile = TopPageViews.class.getClassLoader().getResourceAsStream(schemaFileName);
			schema = new Schema.Parser().parse(schemaFile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		return schema;
	}

	private static Map<String, Object> schemaRegistryConfig(String schemaRegistryUrl) {
		return new HashMap<String, Object>() {
			{
				put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
				put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
			}
		};
	}

}
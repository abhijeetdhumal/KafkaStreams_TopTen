package com.abhijeet.kafkastreams.toppages.utils;

public class UserPageViewConstants {
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
	public static final String AGGREGATION_SCHEMA_FILE = "aggregated.avsc";
	public static final int WINDOW_ADVANCE_TIME = 10;
	public static final int HOPPING_WINDOW_TIME = 60;
}

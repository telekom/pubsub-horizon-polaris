package de.telekom.horizon.polaris;

public class TestConstants {

    public static final String ENV = "ENV";
    public static final String POD_NAME = "POD_NAME";
    public static final String CALLBACK_URL = "/callback_.url"; // % 2 => 0 (should be always 0 for tests with 2 pods)
    public static final String CALLBACK_URL_NEW = "/callback2.url"; // % 2 => 1 (should be different hash module then first callback url)
    public static final String SUBSCRIPTION_ID = "SUB_ID";

    public static final String PUBLISHER_ID="PUBLISHER_ID";
    public static final String SUBSCRIBER_ID="SUBSCRIBER_ID";
    public static final String MESSAGE_ID = "MSG_ID";
    public static final String EVENT_ID = "EVENT_ID";
    public static final String TOPIC = "subscribed";
}

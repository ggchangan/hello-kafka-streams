package io.ggchangan.elasticsearch.sink;


import io.amient.kafka.connect.embedded.ConnectEmbedded;
import io.amient.kafka.connect.irc.IRCFeedConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * https://www.confluent.io/blog/hello-world-kafka-connect-kafka-streams/
 * wikiSource产生新的数据会被立即插入到elasicSink中
 */
public class WikipediaElasticsearchSinkDemo {
    private static final Logger log = LoggerFactory.getLogger(WikipediaElasticsearchSinkDemo.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String DEFAULT_ELASTICSEARCH_SERVERS = "localhost:9200";

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length >= 1 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;

        //1. Launch Embedded Connect Instance for ingesting Wikipedia IRC feed into wikipedia-raw topic
        ConnectEmbedded wikiSource = createWikipediaFeedConnectInstance(bootstrapServers);
        wikiSource.start();

        //2. Lauch Embedded Conect Instance for elasticsearch sink
        final String elasticsearchServers = args.length > 1 ? args[1] : DEFAULT_ELASTICSEARCH_SERVERS;
        ConnectEmbedded elasticSink = createElasticsearchSinkConnectInstance(bootstrapServers, elasticsearchServers);
        try {
            elasticSink.start();
        } catch (Throwable e) {
            log.error("Stopping the application due to wiki Source initialization error ", e);
            wikiSource.stop();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                wikiSource.stop();
            }
        });

        try {
            wikiSource.awaitStop();
            log.info("Connect closed cleanly...");
        } finally {
            elasticSink.stop();
            log.info("Elasticsearch Sink closed cleanly...");
        }
    }

    private static ConnectEmbedded createElasticsearchSinkConnectInstance(String bootstrapServers, String elasticsearchServers) throws Exception {
        Properties workerProps = new Properties();
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "elasticsearch-sink-group");
        workerProps.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "elasticsearch-connect-offsets");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "elsticsearch-connect-configs");
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "elasticsearch-connect-status");
        workerProps.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000");
        workerProps.put(DistributedConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "false");

        Properties connectorProps = new Properties();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, "elasticsearch-sink");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector");
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "10");
        connectorProps.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, elasticsearchServers);
        //connectorProps.put("topics", "test-elasticsearch-sink");
        connectorProps.put("topics", "wikipedia-raw");
        connectorProps.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, true);
        connectorProps.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
        //设置忽略schema推断，否则，因无法推断出mapping而使得数据插入失败
        connectorProps.put(ElasticsearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG, true);

        return new ConnectEmbedded(workerProps, connectorProps);
    }

    private static ConnectEmbedded createWikipediaFeedConnectInstance(String bootstrapServers) throws Exception {
        Properties workerProps = new Properties();
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "wikipedia-connect");
        workerProps.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        workerProps.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000");
        workerProps.put(DistributedConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "false");

        Properties connectorProps = new Properties();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, "wikipedia-irc-source");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "io.amient.kafka.connect.irc.IRCFeedConnector");
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "10");
        connectorProps.put(IRCFeedConnector.IRC_HOST_CONFIG, "irc.wikimedia.org");
        connectorProps.put(IRCFeedConnector.IRC_PORT_CONFIG, "6667");
        connectorProps.put(IRCFeedConnector.IRC_CHANNELS_CONFIG, "#en.wikipedia,#en.wiktionary,#en.wikinews");
        connectorProps.put(IRCFeedConnector.TOPIC_CONFIG, "wikipedia-raw");

        return new ConnectEmbedded(workerProps, connectorProps);

    }

}

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

mod consumer_offsets;

pub use consumer_offsets::{
    fetch_consumer_offset_state, ClusterConsumerOffsetState, OffsetMap, TopicState,
};

const DEFAULT_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug)]
pub struct ClusterSummary {
    pub brokers: Vec<BrokerSummary>,
    pub topics: HashMap<String, TopicDetails>,
}

#[derive(Debug)]
pub struct BrokerSummary {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct TopicDetails {
    pub name: String,
    pub replicas: HashSet<i32>,
    pub partitions: OffsetMap,
}

pub fn fetch_cluster_metadata(brokers: &str) -> ClusterSummary {
    let timeout = Duration::from_millis(DEFAULT_TIMEOUT_MS);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(None, timeout)
        .expect("Failed to fetch metadata");

    let brokers: Vec<BrokerSummary> = metadata
        .brokers()
        .iter()
        .map(|broker| BrokerSummary {
            id: broker.id(),
            host: broker.host().to_string(),
            port: broker.port(),
        })
        .collect();

    let topics: HashMap<String, TopicDetails> = metadata
        .topics()
        .iter()
        .map(|topic| {
            // TODO: check topic.error()

            let mut topic_replicas = HashSet::new();
            let mut topic_partitions = OffsetMap::create_with_count(topic.partitions().len());

            for partition in topic.partitions() {
                for replica in partition.replicas() {
                    topic_replicas.insert(*replica);
                }

                let (_low_watermark, high_watermark) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .unwrap_or((-1, -1));
                topic_partitions.set(partition.id(), high_watermark);
            }

            let details = TopicDetails {
                name: topic.name().to_string(),
                replicas: topic_replicas,
                partitions: topic_partitions,
            };

            (topic.name().to_string(), details)
        })
        .collect();

    ClusterSummary {
        brokers: brokers,
        topics: topics,
    }
}

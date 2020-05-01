use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::HashSet;
use std::time::Duration;

const DEFAULT_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug)]
pub struct ClusterSummary {
    pub brokers: Vec<BrokerSummary>,
    pub topics: Vec<TopicSummary>,
}

#[derive(Debug)]
pub struct BrokerSummary {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug)]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: usize,
    pub replica_count: usize,
    pub offset_sum: i64,
}

#[derive(Debug)]
pub struct TopicDetails {
    pub name: String,
    pub replicas: HashSet<i32>,
    pub partitions: Vec<TopicPartitionDetails>,
}

#[derive(Debug)]
pub struct TopicPartitionDetails {
    pub id: i32,
    pub offset: i64,
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

    let topics: Vec<TopicSummary> = metadata
        .topics()
        .iter()
        .map(|topic| {
            // TODO: check topic.error()

            let offset_sum: i64 = topic.partitions().iter().fold(0, |sum, partition| {
                let (_low_watermark, high_watermark) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .unwrap_or((-1, -1));

                sum + high_watermark
            });

            let mut topic_replicas = HashSet::new();
            for partition in topic.partitions() {
                for replica in partition.replicas() {
                    topic_replicas.insert(replica);
                }
            }

            TopicSummary {
                name: topic.name().to_string(),
                partition_count: topic.partitions().len(),
                replica_count: topic_replicas.len(),
                offset_sum: offset_sum,
            }
        })
        .collect();

    ClusterSummary {
        brokers: brokers,
        topics: topics,
    }
}

pub fn fetch_topic_details(brokers: &str, topic_name: &str) -> TopicDetails {
    let timeout = Duration::from_millis(DEFAULT_TIMEOUT_MS);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(Some(topic_name), timeout)
        .expect("Failed to fetch metadata");

    let topic = metadata.topics().iter().next().unwrap();

    let mut topic_replicas = HashSet::new();
    for partition in topic.partitions() {
        for replica in partition.replicas() {
            topic_replicas.insert(*replica);
        }
    }

    let partitions: Vec<TopicPartitionDetails> = topic
        .partitions()
        .iter()
        .map(|partition| {
            let (_low_watermark, high_watermark) = consumer
                .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                .unwrap_or((-1, -1));

            TopicPartitionDetails {
                id: partition.id(),
                offset: high_watermark,
            }
        })
        .collect();

    TopicDetails {
        name: topic.name().to_string(),
        replicas: topic_replicas,
        partitions: partitions,
    }
}

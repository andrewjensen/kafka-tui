use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;

const DEFAULT_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug)]
pub struct ClusterSummary {
    pub topics: Vec<TopicSummary>,
}

#[derive(Debug)]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: usize,
    pub replica_count: u32,
    pub offset_sum: i64,
}

pub fn fetch_metadata(brokers: &str) -> ClusterSummary {
    let timeout = Duration::from_millis(DEFAULT_TIMEOUT_MS);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(None, timeout)
        .expect("Failed to fetch metadata");

    // TODO: Return cluster-level information too

    // println!("Cluster information:");
    // println!("  Broker count: {}", metadata.brokers().len());
    // println!("  Topics count: {}", metadata.topics().len());
    // println!("  Metadata broker name: {}", metadata.orig_broker_name());
    // println!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    // println!("Brokers:");
    // for broker in metadata.brokers() {
    //     println!(
    //         "  Id: {}  Host: {}:{}  ",
    //         broker.id(),
    //         broker.host(),
    //         broker.port()
    //     );
    // }

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

            // for partition in topic.partitions() {
            //     println!(
            //         "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
            //         partition.id(),
            //         partition.leader(),
            //         partition.replicas(),
            //         partition.isr(),
            //         partition.error()
            //     );
            //     if fetch_offsets {
            //         let (low, high) = consumer
            //             .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
            //             .unwrap_or((-1, -1));
            //         println!(
            //             "       Low watermark: {}  High watermark: {} (difference: {})",
            //             low,
            //             high,
            //             high - low
            //         );
            //         message_count += high - low;
            //     }
            // }

            // TODO: take the max of replicas for all partitions

            TopicSummary {
                name: topic.name().to_string(),
                partition_count: topic.partitions().len(),
                replica_count: 555,
                offset_sum: offset_sum,
            }
        })
        .collect();

    ClusterSummary { topics: topics }
}

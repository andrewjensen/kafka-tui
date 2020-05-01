use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;

mod formatting;
mod kafka;

use formatting::format_padded;
use kafka::{
    fetch_cluster_metadata, fetch_topic_details, TopicDetails, TopicPartitionDetails, TopicSummary,
};

const MOCK_BROKERS: &str = "localhost:9092";

// For summary view
const CHARS_TOPIC_NAME: usize = 40;
const CHARS_PARTITION_COUNT: usize = 11;
const CHARS_REPLICA_COUNT: usize = 9;
const CHARS_SUM_OFFSETS: usize = 9;

// For topic view
const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;

fn main() {
    println!("Fetching cluster metadata...");

    let cluster_summary = fetch_cluster_metadata(MOCK_BROKERS);

    let mut topics = cluster_summary.topics;
    topics.sort_by(|a, b| a.name.cmp(&b.name));

    let mut siv = cursive::Cursive::default();

    siv.add_global_callback('q', |s| s.quit());

    let summary_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Cluster").effect(Effect::Bold))
            .child(TextView::new(format_cluster_summary(
                topics.len(),
                cluster_summary.brokers.len(),
            )))
            .child(DummyView)
            .child(TextView::new(format_topic_list_headers()).effect(Effect::Bold))
            .child(ScrollView::new(
                SelectView::new()
                    .with_all(
                        topics
                            .iter()
                            .map(|topic| (format_topic_summary(topic), topic.name.clone())),
                    )
                    .on_submit(on_select_topic),
            )),
    )
    .title("Kafka");

    siv.add_layer(summary_view);

    siv.run();
}

// Summary View helpers

fn on_select_topic(s: &mut Cursive, topic_name: &str) {
    let topic = fetch_topic_details(MOCK_BROKERS, topic_name);

    let topic_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Topic").effect(Effect::Bold))
            .child(TextView::new(format_topic_details(&topic)))
            .child(DummyView)
            .child(TextView::new(format_partition_list_headers()).effect(Effect::Bold))
            .child(TextView::new(format_partition_list(&topic.partitions))),
    )
    .title(format!("Topic: {}", topic_name))
    .button("Back", |s| {
        s.pop_layer();
    });

    s.add_layer(topic_view);
}

fn format_cluster_summary(topic_count: usize, broker_count: usize) -> String {
    format!("Topics: {}\nBrokers: {}", topic_count, broker_count)
}

fn format_topic_list_headers() -> String {
    let name_fmt = format_padded("Topic", CHARS_TOPIC_NAME);
    let partitions_fmt = format_padded("Partitions", CHARS_PARTITION_COUNT);
    let replicas_fmt = format_padded("Replicas", CHARS_REPLICA_COUNT);
    let offsets_fmt = format_padded("Offsets", CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

fn format_topic_summary(topic: &TopicSummary) -> String {
    let name_fmt = format_padded(&topic.name, CHARS_TOPIC_NAME);
    let partitions_fmt = format_padded(&topic.partition_count.to_string(), CHARS_PARTITION_COUNT);
    let replicas_fmt = format_padded(&topic.replica_count.to_string(), CHARS_REPLICA_COUNT);
    let offsets_fmt = format_padded(&topic.offset_sum.to_string(), CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

// Topic View helpers

fn format_topic_details(topic: &TopicDetails) -> String {
    let replica_count = topic.replicas.len();
    let partition_count = topic.partitions.len();
    let sum_offsets: i64 = topic
        .partitions
        .iter()
        .fold(0, |sum, partition| partition.offset + sum);
    format!(
        "Replication: {}\nPartitions: {}\nSum of Offsets: {}",
        replica_count, partition_count, sum_offsets
    )
}

fn format_partition_list_headers() -> String {
    let id_fmt = format_padded("Partition", CHARS_PARTITION_ID);
    let offset_fmt = format_padded("Offset", CHARS_PARTITION_OFFSET);

    format!("{} {}", id_fmt, offset_fmt)
}

fn format_partition_list(partitions: &Vec<TopicPartitionDetails>) -> String {
    let result_lines: Vec<String> = partitions
        .iter()
        .map(|partition| {
            let id_fmt = format_padded(&partition.id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&partition.offset.to_string(), CHARS_PARTITION_OFFSET);

            format!("{} {}", id_fmt, offset_fmt)
        })
        .collect();

    result_lines.join("\n")
}

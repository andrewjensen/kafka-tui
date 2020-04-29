use cursive::views::{Dialog, LinearLayout, SelectView, TextView};
use cursive::Cursive;
use std::time::Duration;

mod formatting;
mod kafka;

use formatting::format_padded;
use kafka::{fetch_metadata, ClusterSummary, TopicSummary};

const CHARS_TOPIC_NAME: usize = 40;
const CHARS_PARTITION_COUNT: usize = 11;
const CHARS_REPLICA_COUNT: usize = 9;
const CHARS_SUM_OFFSETS: usize = 9;

fn main() {
    let brokers = "localhost:9092";

    println!("Fetching cluster metadata...");

    let cluster_summary = fetch_metadata(brokers);

    // println!("cluster summary:");
    // println!("{:#?}", cluster_summary);

    let topics = cluster_summary.topics;
    // let topics = get_mock_topics();

    let mut siv = cursive::Cursive::default();

    siv.add_global_callback('q', |s| s.quit());

    let summary_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new(format_headers()))
            .child(
                SelectView::new()
                    .with_all(
                        topics
                            .iter()
                            .map(|topic| (format_topic_summary(topic), "test")),
                    )
                    .on_submit(on_select_topic),
            ),
    )
    .title("Kafka");

    siv.add_layer(summary_view);

    siv.run();
}

fn on_select_topic(s: &mut Cursive, _id: &str) {
    let topic_view = Dialog::around(TextView::new("TODO: topic view"))
        .title("Topic: topic.name.here")
        .button("Back", |s| {
            s.pop_layer();
        });

    s.add_layer(topic_view);
}

fn get_mock_topics() -> Vec<TopicSummary> {
    vec![
        TopicSummary {
            name: "topic.one".to_string(),
            partition_count: 10,
            replica_count: 1,
            offset_sum: 2_637,
        },
        TopicSummary {
            name: "my_cool_topic.v2".to_string(),
            partition_count: 50,
            replica_count: 3,
            offset_sum: 12_345_678,
        },
        TopicSummary {
            name: "facts.from.the.legacy.system.v10".to_string(),
            partition_count: 20,
            replica_count: 3,
            offset_sum: 41_737,
        },
    ]
}

fn format_headers() -> String {
    let name_fmt = format_padded("Topic name", CHARS_TOPIC_NAME);
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

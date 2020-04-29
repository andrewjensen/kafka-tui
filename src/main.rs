use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;

mod formatting;
mod kafka;

use formatting::format_padded;
use kafka::{fetch_metadata, TopicSummary};

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

    let mut topics = cluster_summary.topics;
    topics.sort_by(|a, b| a.name.cmp(&b.name));

    // let topics = get_mock_topics();

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
                            .map(|topic| (format_topic_summary(topic), "test")),
                    )
                    .on_submit(on_select_topic),
            )),
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

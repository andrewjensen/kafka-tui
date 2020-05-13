use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;
use std::sync::Arc;

use crate::formatting::format_padded;
use crate::kafka::TopicDetails;
use crate::tui::render_topic_view;
use crate::Model;

const CHARS_TOPIC_NAME: usize = 40;
const CHARS_PARTITION_COUNT: usize = 11;
const CHARS_REPLICA_COUNT: usize = 9;
const CHARS_SUM_OFFSETS: usize = 9;

pub fn render_summary_view(siv: &mut Cursive) {
    let model = siv.user_data::<Model>().unwrap();
    let model_ref = Arc::clone(&model);
    let model_inner = model_ref.lock().unwrap();

    let mut topics: Vec<&TopicDetails> = model_inner
        .cluster_summary
        .topics
        .iter()
        .map(|(_topic_name, topic)| topic)
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));

    let summary_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Cluster").effect(Effect::Bold))
            .child(TextView::new(format_cluster_summary(
                topics.len(),
                model_inner.cluster_summary.brokers.len(),
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
}

fn on_select_topic(siv: &mut Cursive, topic_name: &str) {
    render_topic_view(siv, topic_name);
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

fn format_topic_summary(topic: &TopicDetails) -> String {
    let partition_count = topic.partitions.partition_count.unwrap();
    let summed_offsets = topic.partitions.get_summed_offsets();

    let name_fmt = format_padded(&topic.name, CHARS_TOPIC_NAME);
    let partitions_fmt = format_padded(&partition_count.to_string(), CHARS_PARTITION_COUNT);
    let replicas_fmt = format_padded(&topic.replicas.len().to_string(), CHARS_REPLICA_COUNT);
    let offsets_fmt = format_padded(&summed_offsets.to_string(), CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

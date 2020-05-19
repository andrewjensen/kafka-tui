use cursive::theme::Effect;
use cursive::view::{Boxable, View};
use cursive::views::{
    Canvas, DummyView, LinearLayout, ResizedView, ScrollView, SelectView, TextView,
};
use cursive::Cursive;
use std::collections::HashMap;
use std::sync::Arc;

use crate::formatting::format_padded;
use crate::kafka::TopicDetails;
use crate::tui::render_topic_view;
use crate::Model;

const CHARS_TOPIC_NAME: usize = 40;
const CHARS_PARTITION_COUNT: usize = 11;
const CHARS_REPLICA_COUNT: usize = 9;
const CHARS_SUM_OFFSETS: usize = 9;

struct TopicSummary {
    name: String,
    partition_count: usize,
    replica_count: usize,
    summed_offsets: i64,
}

pub fn render_summary_view(siv: &mut Cursive) {
    let model = siv.user_data::<Model>().unwrap();
    let model_ref = Arc::clone(&model);
    let model_inner = model_ref.lock().unwrap();

    let topic_summaries_by_name: HashMap<String, TopicSummary> = model_inner
        .cluster_summary
        .topics
        .iter()
        .map(|(topic_name, topic)| {
            let partition_count = topic.partitions.partition_count.unwrap();
            let summed_offsets = topic.partitions.get_summed_offsets();

            let topic_summary = TopicSummary {
                name: topic_name.clone(),
                partition_count: partition_count,
                replica_count: topic.replicas.len(),
                summed_offsets: summed_offsets,
            };

            (topic_name.clone(), topic_summary)
        })
        .collect();

    let mut topic_names: Vec<String> = topic_summaries_by_name
        .iter()
        .map(|(topic_name, _topic)| topic_name.clone())
        .collect();
    topic_names.sort();

    let topics_table_headers = Canvas::wrap(TextView::new(" ").effect(Effect::Bold))
        .with_layout(move |view, size| {
            // Subtract the width of the scrollbar and padding
            let available_width = size.x - 2;
            view.set_content(format_topic_list_headers(available_width));
            view.layout(size);
        })
        .full_width();

    let topics_table = ScrollView::new(
        Canvas::wrap(
            SelectView::new()
                .with_all(
                    topic_names
                        .iter()
                        .map(|topic_name| ("", topic_name.clone())),
                )
                .on_submit(on_select_topic),
        )
        .with_layout(move |view, size| {
            let available_width = size.x;

            // Update the label for each item, based on the available width
            for idx in 0..view.len() {
                let (topic_label, topic_name) = view.get_item_mut(idx).unwrap();

                let topic_summary = topic_summaries_by_name.get(topic_name).unwrap();

                *topic_label = format_topic_summary(topic_summary, available_width).into();
            }

            view.layout(size);
        })
        .full_width(),
    );

    let summary_view = ResizedView::with_full_screen(
        LinearLayout::vertical()
            .child(TextView::new("Cluster").effect(Effect::Bold))
            .child(TextView::new(format_cluster_summary(
                topic_names.len(),
                model_inner.cluster_summary.brokers.len(),
            )))
            .child(DummyView)
            .child(topics_table_headers)
            .child(topics_table),
    );

    siv.add_fullscreen_layer(summary_view);
}

fn on_select_topic(siv: &mut Cursive, topic_name: &str) {
    render_topic_view(siv, topic_name);
}

fn format_cluster_summary(topic_count: usize, broker_count: usize) -> String {
    format!("Topics: {}\nBrokers: {}", topic_count, broker_count)
}

fn format_topic_list_headers(available_width: usize) -> String {
    let name_width =
        available_width - CHARS_PARTITION_COUNT - CHARS_REPLICA_COUNT - CHARS_SUM_OFFSETS - 3;

    let name_fmt = format_padded("Topic", name_width);
    let partitions_fmt = format_padded("Partitions", CHARS_PARTITION_COUNT);
    let replicas_fmt = format_padded("Replicas", CHARS_REPLICA_COUNT);
    let offsets_fmt = format_padded("Offsets", CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

fn format_topic_summary(topic_summary: &TopicSummary, available_width: usize) -> String {
    let name_width =
        available_width - CHARS_PARTITION_COUNT - CHARS_REPLICA_COUNT - CHARS_SUM_OFFSETS - 3;

    let name_fmt = format_padded(&topic_summary.name, name_width);
    let partitions_fmt = format_padded(
        &topic_summary.partition_count.to_string(),
        CHARS_PARTITION_COUNT,
    );
    let replicas_fmt = format_padded(
        &topic_summary.replica_count.to_string(),
        CHARS_REPLICA_COUNT,
    );
    let offsets_fmt = format_padded(&topic_summary.summed_offsets.to_string(), CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

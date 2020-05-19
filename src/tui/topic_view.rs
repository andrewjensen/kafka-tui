use cursive::theme::Effect;
use cursive::view::{Boxable, View};
use cursive::views::{
    Canvas, DummyView, LinearLayout, ResizedView, ScrollView, SelectView, TextView,
};
use cursive::Cursive;
use std::collections::HashMap;
use std::sync::Arc;

use crate::formatting::format_padded;
use crate::kafka::{OffsetMap, TopicDetails};
use crate::tui::render_consumer_group_view;
use crate::Model;

const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;
const CHARS_CG_SUM_OFFSETS: usize = 9;

struct ConsumerGroupSummary {
    cg_name: String,
    summed_offsets: i64,
}

pub fn render_topic_view(siv: &mut Cursive, topic_name: &str) {
    let model = siv.user_data::<Model>().unwrap();
    let model_ref = Arc::clone(&model);
    let model_inner = model_ref.lock().unwrap();

    let topic: &TopicDetails = model_inner.cluster_summary.topics.get(topic_name).unwrap();
    let topic_cg_state = model_inner
        .cluster_cg_state
        .topics
        .get(&topic_name.to_string());

    let cgs_by_name: HashMap<String, ConsumerGroupSummary> = match topic_cg_state {
        Some(topic_state) => topic_state
            .consumer_group_states
            .iter()
            .map(|(cg_name, cg_offset_map)| {
                let cg_summary = ConsumerGroupSummary {
                    cg_name: cg_name.clone(),
                    summed_offsets: cg_offset_map.get_summed_offsets(),
                };

                (cg_name.clone(), cg_summary)
            })
            .collect(),
        None => HashMap::new(),
    };

    let mut cg_names: Vec<String> = cgs_by_name
        .iter()
        .map(|(cg_name, _cg_summary)| cg_name.clone())
        .collect();
    cg_names.sort();

    let cg_view = match topic_cg_state {
        Some(topic_state) => {
            let cg_table_headers = Canvas::wrap(TextView::new(" ").effect(Effect::Bold))
                .with_layout(move |view, size| {
                    // Subtract the width of the scrollbar and padding
                    let available_width = size.x - 2;
                    view.set_content(format_consumer_group_list_headers(available_width));
                    view.layout(size);
                })
                .full_width();

            let cg_table = ScrollView::new(
                Canvas::wrap(
                    SelectView::new()
                        .with_all(topic_state.consumer_group_states.iter().map(
                            |(cg_name, _cg_offset_map)| {
                                ("", (topic_name.to_string(), cg_name.clone()))
                            },
                        ))
                        .on_submit(on_select_consumer_group),
                )
                .with_layout(move |view, size| {
                    let available_width = size.x;

                    // Update the label for each item, based on the available width
                    for idx in 0..view.len() {
                        let (cg_label, topic_cg_pair) = view.get_item_mut(idx).unwrap();
                        let (_topic_name, cg_name) = topic_cg_pair;

                        let cg_summary = cgs_by_name.get(cg_name).unwrap();

                        *cg_label = format_consumer_group(cg_summary, available_width).into();
                    }

                    view.layout(size);
                })
                .full_width(),
            );

            LinearLayout::vertical()
                .child(cg_table_headers)
                .child(cg_table)
        }

        None => LinearLayout::vertical()
            .child(TextView::new("Consumer Groups").effect(Effect::Bold))
            .child(TextView::new("(None)")),
    };

    let topic_view = ResizedView::with_full_screen(
        LinearLayout::vertical()
            .child(TextView::new(format!("Topic: {}", topic.name)).effect(Effect::Bold))
            .child(TextView::new(format_topic_details(&topic)))
            .child(DummyView)
            .child(TextView::new(format_partition_list_headers()).effect(Effect::Bold))
            .child(ScrollView::new(TextView::new(format_partition_list(
                &topic.partitions,
            ))))
            .child(DummyView)
            .child(cg_view),
    );

    siv.add_fullscreen_layer(topic_view);
}

fn on_select_consumer_group(siv: &mut Cursive, (topic_name, cg_name): &(String, String)) {
    render_consumer_group_view(siv, topic_name, cg_name);
}

fn format_topic_details(topic: &TopicDetails) -> String {
    let replica_count = topic.replicas.len();
    let partition_count = topic.partitions.partition_count.unwrap();
    let sum_offsets: i64 = topic.partitions.get_summed_offsets();
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

fn format_partition_list(partitions: &OffsetMap) -> String {
    let partition_count = partitions.partition_count.unwrap() as i32;

    let result_lines: Vec<String> = (0..partition_count)
        .map(|partition_id| {
            let offset = partitions.get(partition_id);

            let id_fmt = format_padded(&partition_id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&offset.to_string(), CHARS_PARTITION_OFFSET);

            format!("{} {}", id_fmt, offset_fmt)
        })
        .collect();

    result_lines.join("\n")
}

fn format_consumer_group_list_headers(available_width: usize) -> String {
    let name_width = available_width - CHARS_CG_SUM_OFFSETS - 1;

    let name_fmt = format_padded("Consumer Group", name_width);
    let offset_fmt = format_padded("Offsets", CHARS_CG_SUM_OFFSETS);

    format!("{} {}", name_fmt, offset_fmt)
}

fn format_consumer_group(cg_summary: &ConsumerGroupSummary, available_width: usize) -> String {
    let name_width = available_width - CHARS_CG_SUM_OFFSETS - 1;

    let name_fmt = format_padded(&cg_summary.cg_name, name_width);
    let offset_fmt = format_padded(&cg_summary.summed_offsets.to_string(), CHARS_CG_SUM_OFFSETS);

    format!("{} {}", name_fmt, offset_fmt)
}

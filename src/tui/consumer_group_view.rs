use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, TextView};
use cursive::Cursive;

use crate::formatting::format_padded;
use crate::kafka::OffsetMap;

const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;
const CHARS_PARTITION_LAG: usize = 10;

// TODO: pass in topic offsets so we can display partition lag
pub fn render_consumer_group_view(
    siv: &mut Cursive,
    consumer_group_name: &str,
    offset_map: OffsetMap,
) {
    let consumer_group_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Consumer Group").effect(Effect::Bold))
            .child(DummyView)
            .child(
                TextView::new(format_consumer_group_partition_list_headers()).effect(Effect::Bold),
            )
            .child(ScrollView::new(TextView::new(
                format_consumer_group_partition_list(&offset_map),
            ))),
    )
    .title(format!("Consumer Group: {}", consumer_group_name))
    .button("Back", |s| {
        s.pop_layer();
    });

    siv.add_layer(consumer_group_view);
}

fn format_consumer_group_partition_list_headers() -> String {
    let partition_fmt = format_padded("Partition", CHARS_PARTITION_ID);
    let offset_fmt = format_padded("Offset", CHARS_PARTITION_OFFSET);
    let lag_fmt = format_padded("Lag", CHARS_PARTITION_LAG);

    format!("{} {} {}", partition_fmt, offset_fmt, lag_fmt)
}

fn format_consumer_group_partition_list(offset_map: &OffsetMap) -> String {
    // TODO: replace this with the actual topic partition count
    let mock_partition_count = 50;

    let result_lines: Vec<String> = (0..mock_partition_count)
        .map(|partition_id| {
            let cg_offset = offset_map.get(partition_id);

            let partition_fmt = format_padded(&partition_id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&cg_offset.to_string(), CHARS_PARTITION_OFFSET);
            // TODO: show CG lag on each partition
            let lag_fmt = format_padded("TODO", CHARS_PARTITION_LAG);

            format!("{} {} {}", partition_fmt, offset_fmt, lag_fmt)
        })
        .collect();

    result_lines.join("\n")
}

use cursive::Cursive;

mod formatting;
mod kafka;
mod tui;

use kafka::{fetch_cluster_metadata, fetch_consumer_offset_state};
use tui::render_summary_view;

pub const MOCK_BROKERS: &str = "localhost:9092";

#[tokio::main]
async fn main() {
    println!("Fetching cluster metadata...");
    let cluster_summary = fetch_cluster_metadata(MOCK_BROKERS);

    println!("Fetching consumer group states...");
    let cluster_cg_state = fetch_consumer_offset_state().await;

    let mut siv = Cursive::default();

    siv.add_global_callback('q', |s| s.quit());

    render_summary_view(&mut siv, cluster_summary, cluster_cg_state);

    siv.run();
}

use std::time::Instant;

mod kafka;

use kafka::fetch_consumer_offset_state;

#[tokio::main]
async fn main() {
    let time_start = Instant::now();

    let cluster_state = fetch_consumer_offset_state().await;

    let time_end = Instant::now();
    let processing_duration = time_end.duration_since(time_start);

    println!("Done consuming!");
    println!("Time to process: {:?}", processing_duration);
    println!("Cluster state: {:#?}", cluster_state);
}

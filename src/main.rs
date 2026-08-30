use std::path::Path;

use clap::Parser;

use replicator_rs::config::BackgroundJobConfig;
use replicator_rs::consumer::Consumer;
use replicator_rs::store::ReplicationStore;

#[derive(Parser)]
#[command(name = "replicator-rs", about = "Durable replication worker for rftps")]
struct Cli {
    /// Path to the durable store (SQLite) written by `rftps --record-store`
    #[arg(long)]
    store: String,

    /// Path to the replicator config (bg.json style)
    #[arg(long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();

    let config = BackgroundJobConfig::load_from_file(&cli.config)?;
    let store = ReplicationStore::open(Path::new(&cli.store))?;

    let resumed = store.resume_inflight()?;
    if resumed > 0 {
        println!("[Replicator] resumed {} in-flight record(s)", resumed);
    }
    println!(
        "[Replicator] {} pending record(s) in queue",
        store.pending_count()?
    );

    let consumer = Consumer::new(&config, store, None);

    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("\n[Replicator] shutdown requested");
        let _ = tx.send(true);
    });

    consumer.run(rx).await;
    println!("[Replicator] stopped");
    Ok(())
}

use std::path::Path;

use clap::{Parser, Subcommand};

use replicator_rs::config::BackgroundJobConfig;
use replicator_rs::consumer::Consumer;
use replicator_rs::store::ReplicationStore;

#[cfg(feature = "broker")]
use clap::Args;

#[derive(Parser)]
#[command(name = "replicator-rs", about = "Durable replication worker for rftps")]
struct Cli {
    #[cfg(feature = "broker")]
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to the durable store (SQLite) written by `rftps --record-store`
    #[arg(long)]
    store: Option<String>,

    /// Path to the replicator config (bg.json style)
    #[arg(long)]
    config: Option<String>,
}

#[cfg(feature = "broker")]
#[derive(Subcommand)]
enum Command {
    /// Print a fresh hex-encoded Ed25519 device key
    Keygen,
    /// Interactively create a bg.json config
    Init(InitArgs),
}

#[cfg(feature = "broker")]
#[derive(Args)]
struct InitArgs {
    /// Output file (default: bg.json)
    #[arg(long, short = 'o', default_value = "bg.json")]
    output: String,
    /// Overwrite without prompting
    #[arg(long, short = 'f')]
    force: bool,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();

    #[cfg(feature = "broker")]
    if let Some(command) = cli.command {
        match command {
            Command::Keygen => {
                println!("{}", replicator_rs::generate_device_key());
            }
            Command::Init(args) => {
                run_init(&args.output, args.force);
            }
        }
        return Ok(());
    }

    let store = cli.store.ok_or("--store is required")?;
    let config = cli.config.ok_or("--config is required")?;

    let config = BackgroundJobConfig::load_from_file(&config)?;
    let store = ReplicationStore::open(Path::new(&store))?;

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

#[cfg(feature = "broker")]
fn run_init(output: &str, force: bool) {
    use std::io::Write;

    fn prompt(label: &str, default: Option<&str>) -> String {
        print!("{}", label);
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .expect("failed to read input");
        let value = line.trim().to_string();
        if value.is_empty() {
            default.unwrap_or("").to_string()
        } else {
            value
        }
    }

    let device_key = replicator_rs::generate_device_key();

    let url = prompt(
        "broker url [http://127.0.0.1:8700]: ",
        Some("http://127.0.0.1:8700"),
    );
    if url.is_empty() {
        eprintln!("broker url is required");
        std::process::exit(1);
    }

    let hostname = std::env::var("COMPUTERNAME").unwrap_or_else(|_| "replicator".into());
    let device_name = prompt(&format!("device name [{}]: ", hostname), Some(&hostname));

    let timeout_raw = prompt("approval timeout (seconds) [1800]: ", Some("1800"));
    let approval_timeout_secs: u64 = timeout_raw.trim().parse().unwrap_or(1800);

    let ca_cert = prompt("ca cert file for broker TLS [none]: ", Some("none"));
    let ca_cert = if ca_cert.is_empty() || ca_cert.eq_ignore_ascii_case("none") {
        None
    } else {
        Some(ca_cert)
    };

    let danger_raw = prompt("disable broker cert verification? [n]: ", Some("n"));
    let danger = danger_raw.eq_ignore_ascii_case("y") || danger_raw.eq_ignore_ascii_case("yes");
    if danger {
        eprintln!("WARNING: broker TLS certificate verification will be disabled");
    }

    let messages_raw = prompt("print broker messages? [y]: ", Some("y"));
    let broker_messages = messages_raw.eq_ignore_ascii_case("y")
        || messages_raw.eq_ignore_ascii_case("yes")
        || messages_raw.is_empty();

    let immutable_raw = prompt("immutable naming (prevent overwrites)? [y]: ", Some("y"));
    let immutable_naming = immutable_raw.eq_ignore_ascii_case("y")
        || immutable_raw.eq_ignore_ascii_case("yes")
        || immutable_raw.is_empty();

    let config = serde_json::json!({
        "enabled": true,
        "broker": {
            "url": url,
            "device_key": device_key,
            "device_name": device_name,
            "approval_timeout_secs": approval_timeout_secs,
            "ca_cert": ca_cert,
            "danger_disable_cert_verify": danger,
            "broker_messages": broker_messages,
            "immutable_naming": immutable_naming,
        },
    });

    let path = std::path::Path::new(output);
    if path.exists() && !force {
        let answer = prompt(&format!("{} already exists. Overwrite? [y/N]: ", output), Some("n"));
        if !(answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes")) {
            println!("aborted");
            return;
        }
    }

    let pretty = serde_json::to_string_pretty(&config).expect("failed to serialize config");
    if let Err(e) = std::fs::write(output, pretty) {
        eprintln!("failed to write {}: {}", output, e);
        std::process::exit(1);
    }
    println!("Wrote {}", output);
    println!("Register/approve the device in the broker dashboard at http://127.0.0.1:8701/dashboard");
}

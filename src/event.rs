#[derive(Debug, Clone)]
pub enum ReplicatorEvent {
    BrokerStatus {
        status: String,
        message: Option<String>,
    },
    Replication {
        path: String,
        ok: bool,
        error: Option<String>,
    },
}

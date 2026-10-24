use crate::{mining::stratum::Mining, status::runtime::Status, wallet::payments::PaymentWorker};
use grin_util::{Mutex, RwLock};
use log::{Level, LevelFilter, Log, Metadata, Record};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

#[derive(Clone)]
pub(crate) struct Sources {
    pub node: Status,
    pub wallet: Arc<PaymentWorker>,
    pub mining: Arc<Mining>,
}

#[derive(Clone)]
pub(crate) struct Condition {
    pub message: String,
    pub since: Instant,
}

#[derive(Clone)]
pub(crate) struct Event {
    pub time: chrono::DateTime<chrono::Local>,
    pub level: Level,
    pub source: String,
    pub message: String,
}

pub(crate) struct Reporting {
    pub phase: RwLock<&'static str>,
    pub sources: RwLock<Option<Sources>>,
    pub conditions: Mutex<BTreeMap<&'static str, Condition>>,
    pub events: Arc<Mutex<VecDeque<Event>>>,
}

impl Reporting {
    pub fn new(interactive: bool) -> crate::Result<Self> {
        let events = Arc::new(Mutex::new(VecDeque::new()));
        log::set_boxed_logger(Box::new(Logger {
            events: events.clone(),
            interactive,
        }))
        .map_err(|_| crate::Error::Invalid("logger already initialized"))?;
        log::set_max_level(LevelFilter::Info);
        Ok(Self {
            phase: RwLock::new("Starting G2Pool"),
            sources: RwLock::new(None),
            conditions: Mutex::new(BTreeMap::new()),
            events,
        })
    }

    pub fn phase(&self, phase: &'static str) {
        *self.phase.write() = phase;
        log::info!("{phase}");
    }

    pub fn condition(&self, component: &'static str, message: Option<String>) -> bool {
        let mut conditions = self.conditions.lock();
        match message {
            Some(message) => {
                if conditions
                    .get(component)
                    .is_some_and(|old| old.message == message)
                {
                    return false;
                }
                conditions.insert(
                    component,
                    Condition {
                        message,
                        since: Instant::now(),
                    },
                );
                true
            }
            None => conditions.remove(component).is_some(),
        }
    }
}

struct Logger {
    events: Arc<Mutex<VecDeque<Event>>>,
    interactive: bool,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level()
            <= if metadata.target().starts_with("g2pool") {
                Level::Info
            } else {
                Level::Warn
            }
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let time = chrono::Local::now();
        let message = record.args().to_string();
        if !self.interactive {
            eprintln!(
                "{} {} {}: {}",
                time.format("%H:%M:%S"),
                record.level(),
                record.target(),
                message
            );
            return;
        }
        let clean = |text: &str| {
            text.chars()
                .take(2048)
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect()
        };
        let mut events = self.events.lock();
        if events.len() == 200 {
            events.pop_front();
        }
        events.push_back(Event {
            time,
            level: record.level(),
            source: clean(record.target()),
            message: clean(&message),
        });
    }

    fn flush(&self) {}
}

use std::{
    collections::HashMap,
    fs,
    str::FromStr,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{DataEvent, data::{SubscribeBars, SubscribeFundingRates, SubscribeIndexPrices, SubscribeMarkPrices}},
};
use nautilus_core::{UnixNanos, time::{AtomicTime, get_atomic_clock_realtime}};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{Bar, BarType, Data, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate},
    identifiers::{ClientId, InstrumentId},
    types::{Price, Quantity},
};
use redis::{AsyncCommands, streams::{StreamReadOptions, StreamReadReply}};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::config::{decimal, parse_bar_spec, MarketFeedDataClientConfig};

#[derive(Debug, Deserialize)]
struct EventEnvelope {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    #[serde(rename = "eventId")]
    event_id: String,
    channel: String,
    timeframe: String,
    #[serde(rename = "eventTs")]
    event_ts: u64,
    payload: Value,
}

#[derive(Debug)]
struct FeedRecord {
    stream: String,
    id: String,
    event: EventEnvelope,
}

#[derive(Debug)]
pub struct MarketFeedDataClient {
    client_id: ClientId,
    config: MarketFeedDataClientConfig,
    instrument_id: InstrumentId,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    tasks: TaskGroup,
    cancellation: CancellationToken,
    connected: Arc<AtomicBool>,
}

impl MarketFeedDataClient {
    pub fn new(client_id: ClientId, config: MarketFeedDataClientConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            instrument_id: config.instrument_id,
            config,
            client_id,
            data_sender: get_data_event_sender(),
            clock: get_atomic_clock_realtime(),
            tasks: TaskGroup::new(),
            cancellation: CancellationToken::new(),
            connected: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn run_consumer(
        config: MarketFeedDataClientConfig,
        instrument_id: InstrumentId,
        sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
        cancellation: CancellationToken,
        connected: Arc<AtomicBool>,
    ) -> Result<()> {
        let client = redis::Client::open(config.redis_url.clone())?;
        let mut connection = client.get_connection_manager().await?;
        let symbol = format!(
            "{}_{}_{}",
            config.base_currency.code,
            config.quote_currency.code,
            config.settlement_currency.code,
        );
        let keys = vec![
            format!("{}:BINANCE_FUTURES:{symbol}:candle:{}", config.redis_prefix, config.timeframe),
            format!("{}:BINANCE_FUTURES:{symbol}:funding:na", config.redis_prefix),
            format!("{}:BINANCE_FUTURES:{symbol}:index:na", config.redis_prefix),
        ];
        let mut offsets: Vec<String> = keys.iter().map(|key| {
            let saved = config.offset_path.as_ref().and_then(|path| fs::read_to_string(path).ok()).and_then(|raw| serde_json::from_str::<HashMap<String, String>>(&raw).ok());
            saved.and_then(|map| map.get(key).cloned()).unwrap_or_else(|| if config.start_from == "beginning" { "0-0".to_string() } else { "$".to_string() })
        }).collect();
        // Redis' connection manager has a shorter default response timeout;
        // keep the XREAD block below it and poll again to preserve reconnect
        // and cancellation behavior without changing shared Redis settings.
        let options = StreamReadOptions::default().block(100).count(128);

        loop {
            // Redis 瞬断/超时不能终结消费者：5m 闭合 K 线本就稀疏，XREAD 经常空等；
            // 连接管理器的超时以 Err 返回，若用 `?` 直接抛出去，消费者会在第一次超时就死掉
            // （线上实锤：`market-feed consumer stopped: timed out`），后面再有新 bar 也收不到。
            // 这里打点后稍歇继续轮询；只有取消时才退出。连接建立失败仍在循环外 fail-fast。
            let reply: Option<StreamReadReply> = match connection.xread_options(&keys, &offsets, &options).await {
                Ok(reply) => reply,
                Err(error) => {
                    if cancellation.is_cancelled() {
                        break;
                    }
                    log::warn!("market-feed redis read failed, retrying: {error:#}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let Some(reply) = reply else {
                if cancellation.is_cancelled() { break; }
                continue;
            };
            let mut records = Vec::new();
            for stream in reply.keys {
                for item in stream.ids {
                    let payload = item.get::<String>("event").context("market-feed stream entry missing event")?;
                    let field_event_id = item.get::<String>("eventId").context("market-feed stream entry missing eventId")?;
                    let event: EventEnvelope = serde_json::from_str(&payload).context("invalid market-feed event JSON")?;
                    ensure!(event.schema_version == 2, "unsupported market-feed event schema");
                    ensure!(event.event_id == field_event_id, "market-feed eventId envelope mismatch");
                    records.push(FeedRecord { stream: stream.key.clone(), id: item.id, event });
                }
            }
            records.sort_by_key(|record| (record.event.event_ts, record.id.clone()));
            for record in records {
                emit_event(&config, instrument_id, &sender, clock, record.event)?;
                if let Some(index) = keys.iter().position(|key| key == &record.stream) {
                    offsets[index] = record.id;
                }
                persist_offsets(&config, &keys, &offsets)?;
            }
        }
        connected.store(false, Ordering::Release);
        Ok(())
    }
}

fn persist_offsets(config: &MarketFeedDataClientConfig, keys: &[String], offsets: &[String]) -> Result<()> {
    let Some(path) = config.offset_path.as_ref() else { return Ok(()) };
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let map: HashMap<_, _> = keys.iter().cloned().zip(offsets.iter().cloned()).collect();
    let temp = path.with_extension("tmp");
    fs::write(&temp, serde_json::to_vec(&map)?)?;
    fs::rename(temp, path)?;
    Ok(())
}

fn event_ts(event_ts_ms: u64) -> UnixNanos { UnixNanos::from_millis(event_ts_ms) }

fn price(value: Decimal, precision: u8) -> Result<Price> { Price::from_decimal_dp(value, precision).map_err(Into::into) }
fn quantity(value: Decimal, precision: u8) -> Result<Quantity> { Quantity::from_decimal_dp(value, precision).map_err(Into::into) }

fn emit_event(
    config: &MarketFeedDataClientConfig,
    instrument_id: InstrumentId,
    sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
    event: EventEnvelope,
) -> Result<()> {
    let ts_event = event_ts(event.event_ts);
    let ts_init = clock.get_time_ns();
    match event.channel.as_str() {
        "candle" => {
            ensure!(event.timeframe == config.timeframe, "market-feed candle timeframe mismatch");
            ensure!(event.payload.get("closed").and_then(Value::as_bool).unwrap_or(false), "market-feed emitted an open candle");
            let bar_type = BarType::new(instrument_id, parse_bar_spec(&event.timeframe)?, nautilus_model::enums::AggregationSource::External);
            let bar = Bar::new_checked(
                bar_type,
                price(decimal(&event.payload, "open")?, config.price_precision)?,
                price(decimal(&event.payload, "high")?, config.price_precision)?,
                price(decimal(&event.payload, "low")?, config.price_precision)?,
                price(decimal(&event.payload, "close")?, config.price_precision)?,
                quantity(decimal(&event.payload, "volume")?, config.size_precision)?,
                ts_event,
                ts_init,
            )?;
            sender.send(DataEvent::Data(Data::Bar(bar))).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        "funding" => {
            if let Some(mark) = event.payload.get("markPrice").and_then(Value::as_str) {
                let update = MarkPriceUpdate::new(instrument_id, price(Decimal::from_str(mark)?, config.price_precision)?, ts_event, ts_init);
                sender.send(DataEvent::Data(Data::MarkPrice(update))).map_err(|error| anyhow::anyhow!(error.to_string()))?;
            }
            let rate = decimal(&event.payload, "rate")?;
            let next = event.payload.get("nextFundingTs").and_then(Value::as_i64).filter(|value| *value > 0).map(|value| UnixNanos::from_millis(value as u64));
            sender.send(DataEvent::Data(Data::FundingRate(FundingRateUpdate::new(instrument_id, rate, None, next, ts_event, ts_init)))).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        "index" => {
            let update = IndexPriceUpdate::new(instrument_id, price(decimal(&event.payload, "price")?, config.price_precision)?, ts_event, ts_init);
            sender.send(DataEvent::Data(Data::IndexPrice(update))).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        other => anyhow::bail!("unsupported market-feed channel `{other}`"),
    }
    Ok(())
}

#[async_trait::async_trait(?Send)]
impl DataClient for MarketFeedDataClient {
    fn client_id(&self) -> ClientId { self.client_id }
    fn venue(&self) -> Option<nautilus_model::identifiers::Venue> { Some(self.instrument_id.venue) }
    fn start(&mut self) -> Result<()> { Ok(()) }
    fn stop(&mut self) -> Result<()> { self.tasks.begin_shutdown(); self.connected.store(false, Ordering::Release); Ok(()) }
    fn reset(&mut self) -> Result<()> { self.stop() }
    fn dispose(&mut self) -> Result<()> { self.stop() }
    fn is_connected(&self) -> bool { self.connected.load(Ordering::Acquire) }
    fn is_disconnected(&self) -> bool { !self.is_connected() }

    async fn connect(&mut self) -> Result<()> {
        if self.is_connected() { return Ok(()) }
        if !self.tasks.is_open() { self.tasks.start_generation()?; self.cancellation = self.tasks.cancellation_token(); }
        let instrument = self.config.instrument()?;
        self.data_sender.send(DataEvent::Data(Data::Instrument(Box::new(instrument)))).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let config = self.config.clone();
        let instrument_id = self.instrument_id;
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancellation = self.cancellation.clone();
        let connected = self.connected.clone();
        self.tasks.spawn(async move {
            if let Err(error) = Self::run_consumer(config, instrument_id, sender, clock, cancellation, connected.clone()).await {
                log::error!("market-feed consumer stopped: {error:#}");
                connected.store(false, Ordering::Release);
            }
        })?;
        self.connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.tasks.begin_shutdown();
        self.tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
            .context("failed to stop market-feed tasks")?;
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> Result<()> {
        ensure!(
            !cmd.bar_types.is_empty()
                && cmd.bar_types.iter().all(|bar_type| bar_type.instrument_id() == self.instrument_id),
            "market-feed bar subscription instrument mismatch"
        );
        Ok(())
    }
    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> Result<()> { ensure!(!cmd.instrument_ids.is_empty() && cmd.instrument_ids.iter().all(|id| *id == self.instrument_id), "market-feed mark subscription instrument mismatch"); Ok(()) }
    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> Result<()> { ensure!(!cmd.instrument_ids.is_empty() && cmd.instrument_ids.iter().all(|id| *id == self.instrument_id), "market-feed index subscription instrument mismatch"); Ok(()) }
    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> Result<()> { ensure!(!cmd.instrument_ids.is_empty() && cmd.instrument_ids.iter().all(|id| *id == self.instrument_id), "market-feed funding subscription instrument mismatch"); Ok(()) }
}

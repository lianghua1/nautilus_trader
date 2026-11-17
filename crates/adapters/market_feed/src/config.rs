use std::{any::Any, path::PathBuf, str::FromStr};

use anyhow::{Context, Result, ensure};
use nautilus_common::factories::ClientConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::InstrumentClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, Instrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.market_feed", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.market_feed")
)]
pub struct MarketFeedDataClientConfig {
    pub redis_url: String,
    pub redis_prefix: String,
    pub instrument_id: InstrumentId,
    pub raw_symbol: Symbol,
    pub base_currency: Currency,
    pub quote_currency: Currency,
    pub settlement_currency: Currency,
    pub price_precision: u8,
    pub size_precision: u8,
    pub price_increment: Price,
    pub size_increment: Quantity,
    pub timeframe: String,
    pub offset_path: Option<PathBuf>,
    pub start_from: String,
}

impl ClientConfig for MarketFeedDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MarketFeedDataClientConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.redis_url.trim().is_empty(), "redis_url must not be empty");
        ensure!(!self.redis_prefix.trim().is_empty(), "redis_prefix must not be empty");
        ensure!(self.redis_prefix.ends_with("v2"), "redis_prefix must select the v2 feed contract");
        ensure!(self.instrument_id.venue.as_str() == "BINANCE", "market-feed POC only supports BINANCE");
        ensure!(self.instrument_id.symbol.as_str().ends_with("-PERP"), "market-feed POC requires a Binance USD-M perpetual instrument ID");
        ensure!(self.price_precision > 0, "price_precision must be positive");
        ensure!(self.size_precision > 0, "size_precision must be positive");
        ensure!(!self.timeframe.trim().is_empty(), "timeframe must not be empty");
        ensure!(matches!(self.start_from.as_str(), "latest" | "beginning"), "start_from must be latest or beginning");
        Ok(())
    }

    pub fn instrument(&self) -> Result<InstrumentAny> {
        self.validate()?;
        let instrument = CryptoPerpetual::build_checked(
            self.instrument_id,
            self.raw_symbol,
            self.base_currency,
            self.quote_currency,
            self.settlement_currency,
            false,
            self.price_precision,
            self.size_precision,
            self.price_increment,
            self.size_increment,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UnixNanos::default(),
            UnixNanos::default(),
        )
        .context("invalid market-feed CryptoPerpetual metadata")?;
        ensure!(instrument.instrument_class() == InstrumentClass::Swap, "market-feed instrument must be a swap");
        Ok(instrument.into_any())
    }

    pub fn bar_type(&self) -> Result<nautilus_model::data::BarType> {
        let spec = parse_bar_spec(&self.timeframe)?;
        Ok(nautilus_model::data::BarType::new(
            self.instrument_id,
            spec,
            nautilus_model::enums::AggregationSource::External,
        ))
    }
}

pub fn parse_bar_spec(value: &str) -> Result<nautilus_model::data::BarSpecification> {
    let value = value.trim().to_ascii_lowercase();
    let suffix_start = value
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| anyhow::anyhow!("invalid market-feed timeframe `{value}`"))?;
    let (step, suffix) = value.split_at(suffix_start);
    let step = step.parse::<usize>().context("invalid market-feed timeframe step")?;
    ensure!(step > 0, "market-feed timeframe step must be positive");
    let aggregation = match suffix {
        "m" => nautilus_model::enums::BarAggregation::Minute,
        "h" => nautilus_model::enums::BarAggregation::Hour,
        "d" => nautilus_model::enums::BarAggregation::Day,
        "w" => nautilus_model::enums::BarAggregation::Week,
        _ => anyhow::bail!("unsupported market-feed timeframe `{value}`"),
    };
    Ok(nautilus_model::data::BarSpecification::new(
        step,
        aggregation,
        nautilus_model::enums::PriceType::Last,
    ))
}

pub fn decimal(value: &serde_json::Value, field: &str) -> Result<Decimal> {
    let raw = value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("market-feed payload missing string field `{field}`"))?;
    Decimal::from_str(raw).with_context(|| format!("invalid decimal in market-feed field `{field}`"))
}

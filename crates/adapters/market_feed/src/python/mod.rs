use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    config::MarketFeedDataClientConfig,
    factory::MarketFeedDataClientFactory,
};

#[pymethods]
impl MarketFeedDataClientFactory {
    #[new]
    fn py_new() -> Self { Self }

    fn name(&self) -> &'static str { "MARKET_FEED" }
}

#[pymethods]
impl MarketFeedDataClientConfig {
    #[new]
    #[pyo3(signature = (
        redis_url,
        instrument_id,
        raw_symbol,
        base_currency,
        quote_currency,
        settlement_currency,
        price_precision,
        size_precision,
        price_increment,
        size_increment,
        timeframe,
        offset_path=None,
        redis_prefix=None,
        start_from=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        redis_url: String,
        instrument_id: nautilus_model::identifiers::InstrumentId,
        raw_symbol: nautilus_model::identifiers::Symbol,
        base_currency: nautilus_model::types::Currency,
        quote_currency: nautilus_model::types::Currency,
        settlement_currency: nautilus_model::types::Currency,
        price_precision: u8,
        size_precision: u8,
        price_increment: String,
        size_increment: String,
        timeframe: String,
        offset_path: Option<String>,
        redis_prefix: Option<String>,
        start_from: Option<String>,
    ) -> PyResult<Self> {
        let price_increment = price_increment.parse().map_err(to_pyvalue_err)?;
        let size_increment = size_increment.parse().map_err(to_pyvalue_err)?;
        let config = Self {
            redis_url,
            redis_prefix: redis_prefix.unwrap_or_else(|| "md:v2".to_string()),
            instrument_id,
            raw_symbol,
            base_currency,
            quote_currency,
            settlement_currency,
            price_precision,
            size_precision,
            price_increment,
            size_increment,
            timeframe,
            offset_path: offset_path.map(Into::into),
            start_from: start_from.unwrap_or_else(|| "latest".to_string()),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    fn redis_url(&self) -> &str { &self.redis_url }

    #[getter]
    fn redis_prefix(&self) -> &str { &self.redis_prefix }
}

fn extract_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    factory
        .extract::<MarketFeedDataClientFactory>(py)
        .map(|value| Box::new(value) as Box<dyn DataClientFactory>)
        .map_err(|error| to_pyvalue_err(format!("failed to extract MarketFeedDataClientFactory: {error}")))
}

fn extract_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<MarketFeedDataClientConfig>(py)
        .map(|value| Box::new(value) as Box<dyn ClientConfig>)
        .map_err(|error| to_pyvalue_err(format!("failed to extract MarketFeedDataClientConfig: {error}")))
}

#[pymodule]
pub fn market_feed(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<MarketFeedDataClientFactory>()?;
    module.add_class::<MarketFeedDataClientConfig>()?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor("MARKET_FEED".to_string(), extract_factory)
        .map_err(|error| to_pyruntime_err(format!("failed to register market-feed factory: {error}")))?;
    registry
        .register_config_extractor("MarketFeedDataClientConfig".to_string(), extract_config)
        .map_err(|error| to_pyruntime_err(format!("failed to register market-feed config: {error}")))?;
    Ok(())
}

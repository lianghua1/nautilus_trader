use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clock::Clock,
    clients::DataClient,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{config::MarketFeedDataClientConfig, data::MarketFeedDataClient};

#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.market_feed", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.market_feed")
)]
pub struct MarketFeedDataClientFactory;

impl DataClientFactory for MarketFeedDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<MarketFeedDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("invalid config type for MarketFeedDataClientFactory"))?
            .clone();
        Ok(Box::new(MarketFeedDataClient::new(ClientId::from(name), config)?))
    }

    fn name(&self) -> &'static str {
        "MARKET_FEED"
    }

    fn config_type(&self) -> &'static str {
        "MarketFeedDataClientConfig"
    }
}

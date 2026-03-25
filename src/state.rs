use std::sync::Arc;

use crate::protocol::smart_meter::SmartMeterAdapter;
use crate::protocol::ev_charger::EvChargerAdapter;
use crate::protocol::battery::BatteryAdapter;
use crate::protocol::stacks::ocpp::OcppStack;
use crate::protocol::stacks::sunspec::SunSpecStack;
use crate::protocol::stacks::dlms::DlmsStack;
use crate::protocol::stacks::openadr::OpenAdrStack;
use crate::router::Router;

/// Shared application state, injected into Axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub router: Arc<Router>,
    pub smart_meter_adapter: Arc<SmartMeterAdapter>,
    pub ev_charger_adapter: Arc<EvChargerAdapter>,
    pub battery_adapter: Arc<BatteryAdapter>,
    
    // Private Network Protocol Stacks
    pub ocpp_stack: Arc<OcppStack>,
    pub sunspec_stack: Arc<SunSpecStack>,
    pub dlms_stack: Arc<DlmsStack>,
    pub openadr_stack: Arc<OpenAdrStack>,
}

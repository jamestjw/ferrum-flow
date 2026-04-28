use serde::{Deserialize, Serialize};

use crate::analytics::{OfiMetrics, estimate_price_impact};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Bias {
    Long,
    Short,
    Neutral,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Aggressive,
    Passive,
    Neutral,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    EnterLong,
    EnterShort,
    ExitLong,
    WaitPassive,
    ReverseShort,
    ConfirmLongAtVwap,
    NoTrade,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    pub momentum_threshold: f64,
    pub absorption_ratio_threshold: f64,
    pub absorption_price_epsilon: f64,
    pub lambda: f64,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            momentum_threshold: 0.20,
            absorption_ratio_threshold: 3.0,
            absorption_price_epsilon: 0.01,
            lambda: 0.0001,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalDecision {
    pub bias: Bias,
    pub execution: ExecutionMode,
    pub action: Action,
    pub expected_price_change: f64,
    pub absorption_detected: bool,
}

pub fn evaluate_signal(
    metrics: &OfiMetrics,
    price_change: Option<f64>,
    vwap_context: Option<(f64, f64)>,
    config: &SignalConfig,
) -> SignalDecision {
    let expected_price_change = estimate_price_impact(metrics.ofi, config.lambda);
    let abs_ratio = if metrics.ofi.abs() > 0.0 {
        metrics.total_volume / metrics.ofi.abs()
    } else {
        f64::INFINITY
    };

    let absorption_detected = metrics.normalized_ofi >= config.momentum_threshold
        && abs_ratio >= config.absorption_ratio_threshold
        && price_change
            .map(|delta| delta.abs() <= config.absorption_price_epsilon)
            .unwrap_or(false);

    if absorption_detected {
        return SignalDecision {
            bias: Bias::Short,
            execution: ExecutionMode::Aggressive,
            action: Action::ReverseShort,
            expected_price_change,
            absorption_detected,
        };
    }

    if let Some((last_price, vwap)) = vwap_context {
        if (last_price - vwap).abs() <= config.absorption_price_epsilon
            && metrics.normalized_ofi >= config.momentum_threshold
        {
            return SignalDecision {
                bias: Bias::Long,
                execution: ExecutionMode::Aggressive,
                action: Action::ConfirmLongAtVwap,
                expected_price_change,
                absorption_detected,
            };
        }
    }

    if metrics.normalized_ofi >= config.momentum_threshold {
        return SignalDecision {
            bias: Bias::Long,
            execution: ExecutionMode::Aggressive,
            action: Action::EnterLong,
            expected_price_change,
            absorption_detected,
        };
    }

    if metrics.normalized_ofi <= -config.momentum_threshold {
        return SignalDecision {
            bias: Bias::Short,
            execution: ExecutionMode::Passive,
            action: Action::EnterShort,
            expected_price_change,
            absorption_detected,
        };
    }

    SignalDecision {
        bias: Bias::Neutral,
        execution: ExecutionMode::Neutral,
        action: Action::NoTrade,
        expected_price_change,
        absorption_detected,
    }
}

#[cfg(test)]
mod tests {
    use crate::analytics::OfiMetrics;

    use super::{Action, Bias, ExecutionMode, SignalConfig, evaluate_signal};

    #[test]
    fn signals_long_momentum() {
        let decision = evaluate_signal(
            &OfiMetrics {
                ofi: 40.0,
                total_volume: 100.0,
                normalized_ofi: 0.4,
            },
            Some(0.2),
            None,
            &SignalConfig::default(),
        );

        assert_eq!(decision.bias, Bias::Long);
        assert_eq!(decision.execution, ExecutionMode::Aggressive);
        assert_eq!(decision.action, Action::EnterLong);
    }

    #[test]
    fn detects_absorption_reversal() {
        let decision = evaluate_signal(
            &OfiMetrics {
                ofi: 25.0,
                total_volume: 100.0,
                normalized_ofi: 0.25,
            },
            Some(0.0),
            None,
            &SignalConfig::default(),
        );

        assert!(decision.absorption_detected);
        assert_eq!(decision.action, Action::ReverseShort);
    }

    #[test]
    fn confirms_vwap_entry() {
        let decision = evaluate_signal(
            &OfiMetrics {
                ofi: 30.0,
                total_volume: 100.0,
                normalized_ofi: 0.3,
            },
            Some(0.05),
            Some((100.0, 100.005)),
            &SignalConfig::default(),
        );

        assert_eq!(decision.action, Action::ConfirmLongAtVwap);
    }
}

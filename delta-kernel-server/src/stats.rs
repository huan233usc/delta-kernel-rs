//! Process-wide counters (exposed through `GetDebugStats`) and injected faults (`InjectFault`).

use std::sync::Mutex;
use std::sync::atomic::AtomicI64;

use crate::proto;

#[derive(Default)]
pub struct ServerCounters {
    /// Snapshot builds (full or incremental) started.
    pub builds_started: AtomicI64,
    /// Requests that joined an in-flight build instead of starting one.
    pub builds_coalesced: AtomicI64,
    /// Op sends to client streams (a handed-over op counts once per stream it was sent to).
    pub ops_issued_total: AtomicI64,
}

/// One-shot faults armed through `InjectFault`. Each fault fires at most once and is then cleared.
#[derive(Default)]
pub struct Faults {
    inner: Mutex<FaultState>,
}

#[derive(Default)]
struct FaultState {
    delay_before_response_ms: i64,
    /// `Some((n, mid_op))`: break the stream that is about to carry its (n+1)-th op.
    stream_fault: Option<(i32, bool)>,
}

impl Faults {
    /// Replaces the armed faults. An all-zero spec disarms everything.
    pub fn arm(&self, spec: &proto::FaultSpec) {
        let mut st = self.inner.lock().unwrap();
        st.delay_before_response_ms = spec.delay_before_response_ms.max(0);
        st.stream_fault = (spec.fail_after_n_ops > 0 || spec.kill_stream_mid_op)
            .then_some((spec.fail_after_n_ops.max(0), spec.kill_stream_mid_op));
    }

    /// Consumes the response delay, if armed.
    pub fn take_response_delay_ms(&self) -> i64 {
        std::mem::take(&mut self.inner.lock().unwrap().delay_before_response_ms)
    }

    /// Called before a stream that has already carried `ops_before` ops sends another one.
    ///
    /// Returns `Some(false)` to close the stream before sending (the op was never delivered),
    /// `Some(true)` to send the op and then close the stream while it is in flight, and `None` to
    /// proceed normally.
    pub fn take_stream_fault(&self, ops_before: i32) -> Option<bool> {
        let mut st = self.inner.lock().unwrap();
        match st.stream_fault {
            Some((n, mid_op)) if n == ops_before => {
                st.stream_fault = None;
                Some(mid_op)
            }
            _ => None,
        }
    }
}

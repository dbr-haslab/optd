use datafusion::common::{config::ConfigExtension, extensions_options};

extensions_options! {
    /// Configuration for optd optimizer behavior in DataFusion sessions.
    pub struct OptdExtensionConfig {
        /// Whether SQL execution should route through optd before DataFusion.
        pub optd_enabled: bool, default = true

        /// Whether explain-step logging should be enabled by default.
        pub log_explain_steps: bool, default = true

        /// Whether optd should execute optimized IR through direct DataFusion physical planning.
        pub physical_planning: bool, default = true

        /// Cost of bringing one row from a remote source to the local engine, in the units of
        /// optd's default cost model.
        pub transfer_cost_per_row: f64, default = 1.0

        /// Cost of work at a remote source relative to the same work done locally, as costed by
        /// optd's default cost model.
        pub remote_cost_factor: f64, default = 1.0
    }
}

impl ConfigExtension for OptdExtensionConfig {
    const PREFIX: &'static str = "optd";
}

pub mod proto {
    pub mod v1alpha {
        // Expose the 'cardano' module
        pub mod cardano {
            pub use utxorpc_spec::utxorpc::v1alpha::cardano::*;
        }

        // Expose the 'sync' module
        pub mod sync {
            pub use utxorpc_spec::utxorpc::v1alpha::sync::*;
        }

        // Expose the 'watch' module
        pub mod watch {
            pub use utxorpc_spec::utxorpc::v1alpha::watch::*;
        }

        // Expose the 'submit' module
        pub mod submit {
            pub use utxorpc_spec::utxorpc::v1alpha::submit::*;
        }

        // Expose the 'build' module
        pub mod build {
            pub use utxorpc_spec::utxorpc::v1alpha::build::*;
        }
    }
}

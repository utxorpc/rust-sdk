pub mod proto {
    pub mod cardano {
        pub use utxorpc_spec::utxorpc::v1alpha::cardano;
    }

    // pub mod build {
    //     pub use utxorpc_spec_build::utxorpc::build::v1;
    // }

    pub mod sync {
        pub use utxorpc_spec::utxorpc::v1alpha::sync;
    }

    // pub mod submit {
    //     pub use utxorpc_spec_submit::utxorpc::submit::v1;
    // }

    // pub mod watch {
    //     pub use utxorpc_spec_watch::utxorpc::watch::v1;
    // }
}

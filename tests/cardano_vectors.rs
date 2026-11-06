//! Decodes the Cardano vectors with the spec's generated types.

use prost::Message;
use std::fs;
use std::path::Path;
use utxorpc_spec::utxorpc::{v1alpha, v1beta};

struct Vector {
    name: String,
    slot: u64,
    hash: String,
    tx_count: usize,
    first_tx_hash: Option<String>,
}

fn vectors_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/cardano")
}

fn vectors() -> Vec<Vector> {
    let index = fs::read_to_string(vectors_dir().join("vectors.tsv")).unwrap();

    index
        .lines()
        .skip(1)
        .map(|line| {
            let cols: Vec<_> = line.split('\t').collect();
            Vector {
                name: cols[0].to_string(),
                slot: cols[3].parse().unwrap(),
                hash: cols[4].to_string(),
                tx_count: cols[5].parse().unwrap(),
                first_tx_hash: (cols[6] != "-").then(|| cols[6].to_string()),
            }
        })
        .collect()
}

macro_rules! check_version {
    ($test:ident, $module:ident) => {
        #[test]
        fn $test() {
            let vectors = vectors();
            assert!(!vectors.is_empty());

            for vector in vectors {
                let path = vectors_dir()
                    .join(stringify!($module))
                    .join(format!("{}.pb", vector.name));
                let bytes = fs::read(path).unwrap();

                let block = $module::cardano::Block::decode(bytes.as_slice()).unwrap();
                let header = block.header.as_ref().unwrap();
                let txs = block.body.as_ref().map(|b| b.tx.as_slice()).unwrap_or(&[]);

                assert_eq!(header.slot, vector.slot, "{}", vector.name);
                assert_eq!(hex::encode(&header.hash), vector.hash, "{}", vector.name);
                assert_eq!(txs.len(), vector.tx_count, "{}", vector.name);
                assert_eq!(
                    txs.first().map(|tx| hex::encode(&tx.hash)),
                    vector.first_tx_hash,
                    "{}",
                    vector.name
                );
                assert_eq!(block.encode_to_vec(), bytes, "{}", vector.name);
            }
        }
    };
}

check_version!(decodes_v1alpha, v1alpha);
check_version!(decodes_v1beta, v1beta);

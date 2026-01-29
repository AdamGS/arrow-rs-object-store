use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::Rng;
use std::ops::Range;
use tempfile::TempDir;

const FILE_SIZE: u64 = 64 * 1024 * 1024; // 64 MB
const RANGE_SIZE: u64 = 8 * 1024; // 8 KB ranges

fn generate_random_ranges(file_size: u64, range_size: u64, count: usize) -> Vec<Range<u64>> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            let start = rng.random_range(0..file_size - range_size);
            start..start + range_size
        })
        .collect()
}

fn bench_read_ranges(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Set up the test file
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
    let path = Path::from("bench_file");

    // Create file with random data
    let data: Vec<u8> = (0..FILE_SIZE).map(|i| (i % 256) as u8).collect();
    rt.block_on(async {
        store.put(&path, data.into()).await.unwrap();
    });

    let mut group = c.benchmark_group("read_ranges");

    for num_ranges in [10, 100, 1000] {
        let ranges = generate_random_ranges(FILE_SIZE, RANGE_SIZE, num_ranges);
        let total_bytes = num_ranges as u64 * RANGE_SIZE;

        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(
            BenchmarkId::new("local_fs", num_ranges),
            &ranges,
            |b, ranges| {
                b.to_async(&rt)
                    .iter(|| async { store.get_ranges(&path, ranges).await.unwrap() });
            },
        );
    }

    group.finish();
}

fn bench_get_opts_whole_file(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
    let path = Path::from("bench_file");

    let data: Vec<u8> = (0..FILE_SIZE).map(|i| (i % 256) as u8).collect();
    rt.block_on(async {
        store.put(&path, data.into()).await.unwrap();
    });

    let mut group = c.benchmark_group("get_opts_whole_file");
    group.throughput(Throughput::Bytes(FILE_SIZE));

    group.bench_function("local_fs", |b| {
        b.to_async(&rt).iter(|| async {
            store
                .get_opts(&path, Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        });
    });

    group.finish();
}

fn bench_get_range_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
    let path = Path::from("bench_file");

    let data: Vec<u8> = (0..FILE_SIZE).map(|i| (i % 256) as u8).collect();
    rt.block_on(async {
        store.put(&path, data.into()).await.unwrap();
    });

    let mut group = c.benchmark_group("get_range_sequential");

    for num_ranges in [10, 100, 1000] {
        let ranges = generate_random_ranges(FILE_SIZE, RANGE_SIZE, num_ranges);
        let total_bytes = num_ranges as u64 * RANGE_SIZE;

        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(
            BenchmarkId::new("local_fs", num_ranges),
            &ranges,
            |b, ranges| {
                b.to_async(&rt).iter(|| async {
                    for range in ranges {
                        store.get_range(&path, range.clone()).await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_read_ranges,
    bench_get_opts_whole_file,
    bench_get_range_sequential
);
criterion_main!(benches);

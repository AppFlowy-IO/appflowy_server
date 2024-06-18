use appflowy_cloud::middleware::feature_migration::FeatureMigrationSource;
use itertools::Itertools;
use sqlx_core::migrate::{Migration, MigrationSource};

fn has_migration(migrations: &[Migration], name: &str) -> bool {
  migrations.iter().any(|m| m.description == name)
}

#[tokio::test]
async fn migrator_with_feature_test() {
  let source = FeatureMigrationSource::with_features(
    "./migrations",
    vec![FeatureMigrationSource::FEATURE_DEFAULT, "ai"],
  );
  let migrations = source.resolve().await.unwrap();
  assert!(
    has_migration(&migrations, "collab embeddings"),
    "ai migrations should be included"
  );
  assert!(
    has_migration(&migrations, "user"),
    "default migrations should be included"
  );
  let sorted_by_date: Vec<_> = migrations.iter().map(|m| m.version).sorted().collect();
  let versions: Vec<_> = migrations.into_iter().map(|m| m.version).collect();
  assert_eq!(
    sorted_by_date, versions,
    "migrations should be sorted by date"
  );
}

#[tokio::test]
async fn migrator_without_feature_test() {
  let source = FeatureMigrationSource::with_features(
    "./migrations",
    vec![FeatureMigrationSource::FEATURE_DEFAULT],
  );
  let migrations = source.resolve().await.unwrap();
  assert!(
    !has_migration(&migrations, "collab embeddings"),
    "ai migrations should NOT be included"
  );
  assert!(
    has_migration(&migrations, "user"),
    "default migrations should be included"
  );
}

#[tokio::test]
async fn migrator_no_default_test() {
  let source = FeatureMigrationSource::with_features("./migrations", vec!["ai"]);
  let migrations = source.resolve().await.unwrap();
  assert!(
    has_migration(&migrations, "collab embeddings"),
    "ai migrations should be included"
  );
  assert!(
    !has_migration(&migrations, "user"),
    "default migrations should NOT be included"
  );
}

use std::path::{Path, PathBuf};

use futures_util::future::BoxFuture;
use sqlx::error::BoxDynError;
use sqlx::migrate::{Migration, MigrationSource};

#[derive(Debug)]
pub struct FeatureMigrationSource {
  root: PathBuf,
  features: Vec<&'static str>,
}

impl FeatureMigrationSource {
  pub const FEATURE_DEFAULT: &'static str = "default";

  pub fn new<P: AsRef<Path>>(root: P) -> Self {
    let features = vec![
      Self::FEATURE_DEFAULT,
      #[cfg(feature = "ai")]
      "ai",
    ];
    Self::with_features(root, features)
  }

  pub fn with_features<P: AsRef<Path>>(root: P, features: Vec<&'static str>) -> Self {
    FeatureMigrationSource {
      root: root.as_ref().to_path_buf(),
      features,
    }
  }
}

impl<'s> MigrationSource<'s> for FeatureMigrationSource {
  fn resolve(self) -> BoxFuture<'s, Result<Vec<Migration>, BoxDynError>> {
    Box::pin(async move {
      if !self.root.exists() {
        return Err(format!("Migration path does not exist: {:?}", self.root).into());
      }
      let mut migrations = Vec::new();
      for feature in self.features {
        let path = match feature {
          Self::FEATURE_DEFAULT => self.root.clone(),
          _ => self.root.join(feature),
        };
        if path.exists() && path.is_dir() {
          let feature_migrations: Vec<_> = sqlx_core::migrate::resolve_blocking(path)?
            .into_iter()
            .map(|(migration, _)| migration)
            .collect();
          migrations.extend(feature_migrations);
        }
      }

      migrations.sort_by_key(|m| m.version);
      Ok(migrations)
    })
  }
}

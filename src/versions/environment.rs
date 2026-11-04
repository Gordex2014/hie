//! Which environment a version is saved in: for python-runtime, the
//! consumer's `GET /v1/capabilities` (its `default_environment`, and that
//! entry's `digest` in `environments`), unless the save names one the
//! runtime offers; locally always `local`.

use super::{Environment, Versions};
use crate::manager::{ErrorStatus, ManagerError};

impl Versions {
    /// The environment a version saved now runs in: the one `requested`
    /// names (`409 environment_unavailable` when the executor does not offer
    /// it), else the executor's default (`503 executor_unavailable` when none
    /// can be determined).
    pub async fn environment_for(
        &self,
        requested: Option<&str>,
    ) -> Result<Environment, ManagerError> {
        let Some(facade) = &self.facade else {
            return match requested {
                None | Some("local") => Ok(Environment::local()),
                Some(other) => Err(unavailable(other)),
            };
        };
        let capabilities = facade.capabilities().await.map_err(|e| {
            ManagerError::new(
                ErrorStatus::Unavailable,
                "executor_unavailable",
                format!("the runtime's environments cannot be read: {e:#}"),
            )
        })?;
        if let Some(default) = capabilities.default() {
            *self
                .default_environment
                .lock()
                .expect("default environment") = Some(default);
        }
        match requested {
            Some(id) => capabilities.environment(id).ok_or_else(|| unavailable(id)),
            None => capabilities.default().ok_or_else(|| {
                ManagerError::new(
                    ErrorStatus::Unavailable,
                    "executor_unavailable",
                    "the runtime names no default environment it offers",
                )
            }),
        }
    }

    /// The default environment as last read, for saves that cannot wait
    /// (a file found while a load starts); `None` until the runtime answered.
    /// A missing value is asked for again in the background.
    pub(super) fn known_default_environment(&self) -> Option<Environment> {
        let Some(facade) = &self.facade else {
            return Some(Environment::local());
        };
        let known = self
            .default_environment
            .lock()
            .expect("default environment")
            .clone();
        if known.is_none()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let facade = facade.clone();
            let slot = self.default_environment.clone();
            handle.spawn(async move {
                if let Ok(Some(default)) = facade.capabilities().await.map(|c| c.default()) {
                    *slot.lock().expect("default environment") = Some(default);
                }
            });
        }
        known
    }

    /// Makes sure the transform files found at start are imported: at once
    /// for the local executor; for python-runtime once its default
    /// environment is known (read here when it is not yet). Every reader that
    /// can wait calls it first.
    pub async fn ready(&self) {
        if self.imported.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let known = self
            .default_environment
            .lock()
            .expect("default environment")
            .is_some();
        if self.facade.is_some() && !known {
            let _ = self.environment_for(None).await;
        }
        self.import_dir();
    }
}

fn unavailable(id: &str) -> ManagerError {
    ManagerError::conflict(
        "environment_unavailable",
        format!("the executor does not offer environment {id:?}"),
    )
}

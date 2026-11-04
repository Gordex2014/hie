//! Admission: every connection a definition names is checked against the
//! allowlist of whole connections in `hie.yaml` (`Config::admission`, entries
//! already normalized), inside HIE, before a secret is decrypted or a socket
//! opened (decision 37).
//!
//! Empty until track S fills it.

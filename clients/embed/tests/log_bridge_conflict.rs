//! [`trellis_embed::install_log_bridge`] when something else set the global
//! `tracing` subscriber first. Its own test binary, so its own OS process:
//! the global subscriber can only be set once per process.

#[test]
fn installing_after_another_global_subscriber_is_a_conflict_every_time() {
    assert!(trellis_embed::installed_log_bridge().is_none());
    tracing::subscriber::set_global_default(tracing::subscriber::NoSubscriber::default()).unwrap();
    for _ in 0..2 {
        let err = trellis_embed::install_log_bridge().err().unwrap();
        assert_eq!(err.code, "conflict");
        assert!(err.message.contains("already"), "{}", err.message);
    }
    assert!(trellis_embed::installed_log_bridge().is_none());
}

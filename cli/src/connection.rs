//! Shared database-connection resolution for CLI subcommands.
//!
//! Every subcommand accepts `-d`/`--database-url`, resolved the same way
//! [`trellis::Config::resolve`] already resolves it for any embedder: an
//! explicit connection string if the operator gave one, else
//! `TRELLIS_DATABASE_URL`, else the standard `PGHOST`/`PGPORT`/`PGUSER`/
//! `PGPASSWORD`/`PGDATABASE` environment variables. This module's only job is
//! pulling that explicit connection string out of argv, if present, before
//! handing it to `Config::resolve`.

/// The long spelling of the connection-string flag.
const LONG_FLAG: &str = "--database-url";
/// The short spelling of the connection-string flag.
const SHORT_FLAG: &str = "-d";

/// Scans `args` for `--database-url`/`-d <URL>` and removes it (and its
/// value) if found, returning the value.
///
/// Deliberately position-independent — it finds the flag wherever it sits in
/// `args` — rather than only recognizing it before or only after the
/// subcommand name. That gives an operator the same result whether they
/// write `trellis --database-url <URL> define <GRAMMAR>` or
/// `trellis apply --database-url <URL> <GRAMMAR>`, without this crate
/// needing a general per-subcommand flag-parsing framework: callers just run
/// this once, on the full argv, before splitting off the subcommand name.
///
/// Returns `Ok(None)` if the flag isn't present at all (callers then fall
/// back to `trellis::Config::resolve`'s own env-var chain). Errors if the flag
/// is given with no following value, or given more than once.
///
/// Nothing after a `--` is a flag: it ends the options, so a positional
/// argument spelled like the flag (a key `trellis release` is handed, say)
/// stays where it is.
pub fn extract_database_url(args: &mut Vec<String>) -> Result<Option<String>, String> {
    let is_flag = |a: &String| a == LONG_FLAG || a == SHORT_FLAG;
    let options = |args: &[String]| args.iter().position(|a| a == "--").unwrap_or(args.len());
    let Some(idx) = args[..options(args)].iter().position(is_flag) else {
        return Ok(None);
    };
    if idx + 1 >= options(args) {
        return Err(format!(
            "{} requires a value, e.g. {LONG_FLAG} <URL>",
            args[idx]
        ));
    }
    let flag = args.remove(idx);
    let value = args.remove(idx);

    if let Some(second_idx) = args[..options(args)].iter().position(is_flag) {
        let second_flag = &args[second_idx];
        return Err(format!(
            "{LONG_FLAG}/{SHORT_FLAG} may only be specified once, got both {flag} and {second_flag}"
        ));
    }

    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_flag_returns_none() {
        let mut args = vec!["define".to_string(), "TRANSFORM x FROM y".to_string()];
        let original = args.clone();
        assert_eq!(extract_database_url(&mut args).unwrap(), None);
        assert_eq!(args, original);
    }

    #[test]
    fn long_flag_before_subcommand_is_extracted() {
        let mut args = vec![
            "--database-url".to_string(),
            "postgresql://x/y".to_string(),
            "define".to_string(),
            "TRANSFORM x FROM y".to_string(),
        ];
        let url = extract_database_url(&mut args).unwrap();
        assert_eq!(url.as_deref(), Some("postgresql://x/y"));
        assert_eq!(args, vec!["define", "TRANSFORM x FROM y"]);
    }

    #[test]
    fn short_flag_after_subcommand_is_extracted() {
        let mut args = vec![
            "define".to_string(),
            "-d".to_string(),
            "postgresql://x/y".to_string(),
            "TRANSFORM x FROM y".to_string(),
        ];
        let url = extract_database_url(&mut args).unwrap();
        assert_eq!(url.as_deref(), Some("postgresql://x/y"));
        assert_eq!(args, vec!["define", "TRANSFORM x FROM y"]);
    }

    #[test]
    fn nothing_after_a_double_dash_is_a_flag() {
        let mut args: Vec<String> = [
            "release",
            "-d",
            "postgres://x",
            "--",
            "t",
            "-d",
            "--database-url",
        ]
        .iter()
        .map(|a| a.to_string())
        .collect();
        assert_eq!(
            extract_database_url(&mut args).unwrap().as_deref(),
            Some("postgres://x")
        );
        assert_eq!(args, ["release", "--", "t", "-d", "--database-url"]);

        let mut args: Vec<String> = ["release", "--", "t", "s", "-d"]
            .iter()
            .map(|a| a.to_string())
            .collect();
        assert_eq!(extract_database_url(&mut args).unwrap(), None);

        // The flag's value can't be the `--` itself.
        let mut args: Vec<String> = ["release", "-d", "--", "t"]
            .iter()
            .map(|a| a.to_string())
            .collect();
        assert!(extract_database_url(&mut args).is_err());
    }

    #[test]
    fn flag_with_no_value_is_an_error() {
        let mut args = vec!["define".to_string(), "--database-url".to_string()];
        assert!(extract_database_url(&mut args).is_err());
    }

    #[test]
    fn flag_specified_twice_is_an_error() {
        let mut args = vec![
            "--database-url".to_string(),
            "postgresql://x/y".to_string(),
            "define".to_string(),
            "-d".to_string(),
            "postgresql://a/b".to_string(),
            "TRANSFORM x FROM y".to_string(),
        ];
        let err = extract_database_url(&mut args).unwrap_err();
        assert!(err.contains("--database-url"));
        assert!(err.contains("-d"));
    }

    #[test]
    fn long_flag_specified_twice_reports_the_flags_actually_used() {
        // Both occurrences are `--database-url`; the error must name what
        // was actually typed, not just assume one long and one short flag.
        let mut args = vec![
            "--database-url".to_string(),
            "postgresql://x/y".to_string(),
            "--database-url".to_string(),
            "postgresql://a/b".to_string(),
        ];
        let err = extract_database_url(&mut args).unwrap_err();
        assert!(
            !err.contains("got both --database-url and -d"),
            "error should not fabricate a short flag that was never given: {err}"
        );
    }

    #[test]
    fn short_flag_specified_twice_reports_the_flags_actually_used() {
        // Both occurrences are `-d`; the error must not claim `--database-url`
        // was one of the two flags seen.
        let mut args = vec![
            "-d".to_string(),
            "postgresql://x/y".to_string(),
            "-d".to_string(),
            "postgresql://a/b".to_string(),
        ];
        let err = extract_database_url(&mut args).unwrap_err();
        assert!(
            !err.contains("got both --database-url and -d"),
            "error should not fabricate a long flag that was never given: {err}"
        );
    }
}

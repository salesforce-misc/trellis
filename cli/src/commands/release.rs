//! `trellis release <TRANSFORM> <SOURCE_TABLE> <KEY>` — releases one key a
//! transform holds in quarantine ([`Trellis::release_key`]), once the cause
//! that poisoned it is fixed.
//!
//! Its own command rather than a statement of `apply`'s grammar, because
//! releasing a key changes nothing about the definitions: it is operational,
//! like `status`, which lists the held keys this takes. Like `status`, it
//! never migrates: it acts on keys a running instance holds, so a database
//! without the Trellis schema has nothing to release.

use trellis::{Config, Trellis, TrellisOptions};

/// Help text for `trellis release -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis release [--database-url <URL>] <TRANSFORM> <SOURCE_TABLE> <KEY>

Releases one source key a transform holds in quarantine, once the cause that
poisoned it is fixed. `trellis status` lists the held keys under
\"Quarantined source rows\", as transform=, table= and key=; pass them here:

  trellis release order_totals public.orders 42

<TRANSFORM> is the transform's bare target-table name. <SOURCE_TABLE> may be
`schema.table` or a bare table name. <KEY> is the key as status prints it (a
composite key's columns joined as status shows them). Status quotes each of
the three for a POSIX shell where it needs it ('...', with a ' inside written
'\\''), so paste them as printed.

The release stages a recompute of the key, which every transform reading the
table applies from the key's current row, and discards the changes held for
<TRANSFORM> meanwhile. Another transform holding the same key keeps holding
it. If the cause is still there, the key is poisoned again. Resuming the
transform (`trellis apply 'RESUME TRANSFORM <TRANSFORM>'`) releases every key
it holds.

A transform that doesn't exist, or a key it doesn't hold, is an error, and
nothing changes. The release first waits for the drain pages in flight on the
table to commit; if one holds it past the 30-second lock timeout, the release
fails with a `timeout` error and changes nothing: run it again.

Options:
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// `text` as one word of a POSIX shell command line: bare when every
/// character is one no shell treats specially, otherwise inside single
/// quotes, where nothing is special but the quote itself, written `'\''`
/// (close the quotes, an escaped quote, reopen them). `status` prints held
/// keys, their tables and transforms this way, so a line pasted from it
/// passes `release` each one exactly as the engine reports it (#842).
pub fn shell_quote(text: &str) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || "_-.,:/@%+=".contains(c);
    if !text.is_empty() && text.chars().all(plain) {
        return text.to_string();
    }
    format!("'{}'", text.replace('\'', "'\\''"))
}

/// A parsed `release` invocation.
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub transform: String,
    pub source_table: String,
    pub key: String,
}

/// Parses the remaining args after `--database-url`/`-d` (and the
/// subcommand name itself) have been stripped: exactly three positional
/// arguments.
pub fn parse(args: &[String]) -> Result<Args, String> {
    match args {
        [transform, source_table, key] => Ok(Args {
            transform: transform.clone(),
            source_table: source_table.clone(),
            key: key.clone(),
        }),
        _ => Err(format!(
            "{USAGE}\nerror: expected <TRANSFORM> <SOURCE_TABLE> <KEY>, got {} argument(s): {:?}",
            args.len(),
            args
        )),
    }
}

/// Connects, releases the key, and disconnects, returning a confirmation.
///
/// `shutdown` is called even if the release failed, per
/// [`Trellis::shutdown`]'s "correct lifecycle call" contract, but the
/// release's error takes priority over a shutdown failure when both occur.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;

    let outcome = trellis
        .release_key(&args.transform, &args.source_table, &args.key)
        .await
        .map_err(|err| err.to_string());
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());

    outcome?;
    shutdown_outcome?;
    Ok(format!(
        "released key {} of {} for {}; a recompute re-derives it from its current row",
        shell_quote(&args.key),
        shell_quote(&args.source_table),
        shell_quote(&args.transform)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn three_arguments_parse_in_order() {
        assert_eq!(
            parse(&strings(&["order_totals", "public.orders", "42"])).unwrap(),
            Args {
                transform: "order_totals".to_string(),
                source_table: "public.orders".to_string(),
                key: "42".to_string(),
            }
        );
    }

    #[test]
    fn a_plain_word_is_left_bare() {
        for word in ["42", "public.orders", "order_totals", "a-b,c:d/e@f%g+h=i"] {
            assert_eq!(shell_quote(word), word);
        }
    }

    #[test]
    fn anything_else_is_single_quoted_with_quotes_escaped() {
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("$HOME"), "'$HOME'");
        assert_eq!(shell_quote(r#"a"b\c"#), r#"'a"b\c'"#);
        assert_eq!(shell_quote("it's"), r#"'it'\''s'"#);
    }

    #[test]
    fn any_other_count_is_a_usage_error() {
        for args in [
            &[][..],
            &["order_totals"][..],
            &["order_totals", "public.orders"][..],
            &["order_totals", "public.orders", "42", "43"][..],
        ] {
            let err = parse(&strings(args)).unwrap_err();
            assert!(err.contains("Usage: trellis release"), "{err}");
            assert!(
                err.contains("expected <TRANSFORM> <SOURCE_TABLE> <KEY>"),
                "{err}"
            );
        }
    }
}

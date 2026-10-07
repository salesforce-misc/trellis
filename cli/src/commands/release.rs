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
the three for the shell where it needs it ('...', with a ' inside written
'\\'', and a control character, such as the separator between a composite
key's columns, written $'\\ooo' in octal, which bash and zsh read), so paste
them as printed. Put -- before the three if one of them is -h, --help, -d or
--database-url:

  trellis release -- order_totals public.orders -h

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

/// `text` as one word of a shell command line (#842). It's bare when every
/// character is one no POSIX shell, bash or zsh treats specially anywhere in
/// a word (letters, digits and `_-.,:/@+`; not `=`, which zsh expands at a
/// word's start, nor `%` or `~`). Otherwise it's inside single quotes, where
/// nothing is special but the quote itself, written `'\''` (close the quotes,
/// an escaped quote, reopen them). A control character, such as the
/// separator between a composite key's columns or an escape a source row
/// smuggled into its key, is never written raw: a terminal drops it from a
/// copy, or obeys it. It's a `$'\ooo'` word of its UTF-8 bytes in octal,
/// joined onto the quoted text, which bash and zsh read back as those bytes.
/// `status` prints held keys, their tables and transforms this way, so a
/// line pasted from it passes `release` each one exactly as the engine
/// reports it.
pub fn shell_quote(text: &str) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || "_-.,:/@+".contains(c);
    if !text.is_empty() && text.chars().all(plain) {
        return text.to_string();
    }
    if text.is_empty() {
        return "''".to_string();
    }
    let mut out = String::new();
    let mut in_quotes = false;
    for c in text.chars() {
        if c.is_control() {
            if in_quotes {
                out.push('\'');
                in_quotes = false;
            }
            out.push_str("$'");
            for byte in c.to_string().bytes() {
                out.push_str(&format!("\\{byte:03o}"));
            }
            out.push('\'');
        } else {
            if !in_quotes {
                out.push('\'');
                in_quotes = true;
            }
            if c == '\'' {
                out.push_str("'\\''");
            } else {
                out.push(c);
            }
        }
    }
    if in_quotes {
        out.push('\'');
    }
    out
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
/// arguments, after an optional `--` that lets one of them be `-h`,
/// `--help`, `-d` or `--database-url`.
pub fn parse(args: &[String]) -> Result<Args, String> {
    let args = match args {
        [first, rest @ ..] if first == "--" => rest,
        _ => args,
    };
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
        for word in ["42", "-7", "public.orders", "order_totals", "a-b,c:d/e@f+g"] {
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
        // zsh expands a word starting `=` to a command's path, and `%`/`~`
        // start job and home-directory names in some shells.
        assert_eq!(shell_quote("=ls"), "'=ls'");
        assert_eq!(shell_quote("%1"), "'%1'");
        assert_eq!(shell_quote("~root"), "'~root'");
        assert_eq!(shell_quote("é"), "'é'");
    }

    #[test]
    fn a_control_character_is_written_in_octal_never_raw() {
        // A composite key's separator (U+001F), a NULL column's sentinel
        // (U+0001), an escape (U+001B), a newline and a C1 control (U+0085,
        // two bytes of UTF-8).
        assert_eq!(shell_quote("1\u{1f}2"), r"'1'$'\037''2'");
        assert_eq!(shell_quote("\u{1}"), r"$'\001'");
        assert_eq!(shell_quote("\u{1b}[2J"), r"$'\033''[2J'");
        assert_eq!(shell_quote("a\nb'c"), r"'a'$'\012''b'\''c'");
        assert_eq!(shell_quote("\u{85}"), r"$'\302\205'");
        for word in ["1\u{1f}2", "\u{1b}[2J", "a\nb'c", "\u{85}"] {
            assert!(!shell_quote(word).chars().any(char::is_control), "{word:?}");
        }
    }

    #[test]
    fn a_leading_double_dash_ends_the_options() {
        assert_eq!(
            parse(&strings(&["--", "order_totals", "public.orders", "-h"])).unwrap(),
            Args {
                transform: "order_totals".to_string(),
                source_table: "public.orders".to_string(),
                key: "-h".to_string(),
            }
        );
        assert!(parse(&strings(&["--", "order_totals", "public.orders"])).is_err());
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

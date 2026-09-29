//! Finding a libc locale that makes a session-level locale setting
//! observably hostile to Trellis's pinned output GUCs.
//!
//! `trellis::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` pins `lc_monetary` to
//! `'C'` (issue #672), which renders `money` as `$1,234.56`. `lc_monetary`
//! only accepts a locale the server's C library actually has, and which ones
//! exist varies by box: `C`/`POSIX`/`C.UTF-8` are universal, but they, and
//! `en_US.UTF-8`, all render `$1,234.56` too, so none of them proves a pin
//! works. [`hostile_lc_monetary`] probes a short list of locales that render
//! it differently and returns the first one this server accepts.

/// What `1234.56::money` renders as under the `lc_monetary` Trellis pins.
pub const PINNED_MONEY_TEXT: &str = "$1,234.56";

/// Locales whose `money` rendering differs from [`PINNED_MONEY_TEXT`], in
/// the order [`hostile_lc_monetary`] tries them. glibc normalizes the codeset
/// (`UTF-8` matches an installed `utf8`), so one spelling of each is enough.
const CANDIDATES: &[&str] = &[
    "de_DE.UTF-8",
    "fr_FR.UTF-8",
    "en_GB.UTF-8",
    "en_IN.UTF-8",
    "en_IN",
    "en_DK.UTF-8",
    "ja_JP.UTF-8",
];

/// Returns an `lc_monetary` value this server accepts under which
/// `1234.56::money::text` is *not* [`PINNED_MONEY_TEXT`], leaving `client`'s
/// own `lc_monetary` reset afterwards.
///
/// Panics if the box has none of them installed: a pinning test that
/// silently skips would pass without proving anything. CI generates
/// `de_DE.UTF-8` for this (see `.github/workflows/ci.yml`); locally,
/// `sudo locale-gen de_DE.UTF-8` (Debian/Ubuntu) or
/// `sudo dnf install glibc-langpack-de` (Fedora) does the same.
pub async fn hostile_lc_monetary(client: &tokio_postgres::Client) -> String {
    for candidate in CANDIDATES {
        if client
            .batch_execute(&format!("set lc_monetary to '{candidate}'"))
            .await
            .is_err()
        {
            continue;
        }
        let rendered: String = client
            .query_one("select 1234.56::money::text", &[])
            .await
            .expect("render money under a candidate lc_monetary")
            .get(0);
        client
            .batch_execute("reset lc_monetary")
            .await
            .expect("reset lc_monetary");
        if rendered != PINNED_MONEY_TEXT {
            return (*candidate).to_string();
        }
    }
    panic!(
        "no locale in {CANDIDATES:?} is installed on this box, so no test can \
         prove lc_monetary is pinned; install one (e.g. `sudo locale-gen \
         de_DE.UTF-8` on Debian/Ubuntu, `sudo dnf install glibc-langpack-de` \
         on Fedora)"
    );
}

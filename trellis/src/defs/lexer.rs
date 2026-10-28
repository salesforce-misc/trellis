//! Hand-rolled lexer for the transform-definition grammar. The language is
//! small (keywords, identifiers, numeric literals, a handful of symbols) so
//! a lookup-table lexer + recursive-descent parser is simplest; no
//! parser-combinator crate is needed.

use super::error::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    Number(String),
    /// A single-quoted string literal's decoded contents (quotes stripped,
    /// `''` escapes resolved to a literal `'`), per SQL's spelling.
    String(String),
    Symbol(char),
    /// A run of two or more adjacent operator characters, lexed as Postgres
    /// lexes it (see [`lex`]): `<=`, `>=`, `<>`, `!=`, or a run this grammar
    /// has no operator for (`<=>`, `!=-`, `%-`), which the parser refuses by
    /// name.
    Operator(String),
    Eof,
}

impl Token {
    pub fn describe(&self) -> String {
        match self {
            Token::Ident(s) => format!("identifier '{s}'"),
            Token::Number(s) => format!("number '{s}'"),
            Token::String(s) => format!("string '{s}'"),
            Token::Symbol(c) => format!("'{c}'"),
            Token::Operator(s) => format!("'{s}'"),
            Token::Eof => "end of input".to_string(),
        }
    }
}

const SYMBOLS: &[char] = &[
    '+', '-', '*', '/', '%', '=', ',', '(', ')', '.', '<', '>', '!',
];

/// The characters Postgres lexes into operators (`scan.l`'s `op_chars`).
const OP_CHARS: &str = "~!@#^&|`?+-*/%<>=";

/// Operator characters that let a run end in `+` or `-`.
const OP_RUN_KEEPS_SIGN: &str = "~!@#^&|`?%";

/// The length of the operator run starting at `chars[0]`, by Postgres's
/// rules: the longest stretch of operator characters that stops before a
/// `--` or `/*` comment start, and, unless it holds one of
/// `~ ! @ # ^ & | ` ? %`, gives back its trailing `+` and `-` (down to one
/// character) so `a<-1` is `a < -1` but `a!=-1` is the single operator `!=-`.
fn operator_run_len(chars: &[char]) -> usize {
    let mut n = 0;
    while let Some(&c) = chars.get(n) {
        if !OP_CHARS.contains(c) {
            break;
        }
        let next = chars.get(n + 1);
        if n > 0 && ((c == '-' && next == Some(&'-')) || (c == '/' && next == Some(&'*'))) {
            break;
        }
        n += 1;
    }
    if n > 1
        && matches!(chars[n - 1], '+' | '-')
        && !chars[..n].iter().any(|c| OP_RUN_KEEPS_SIGN.contains(*c))
    {
        while n > 1 && matches!(chars[n - 1], '+' | '-') {
            n -= 1;
        }
    }
    n
}

pub fn lex(input: &str) -> Result<Vec<Token>, ParseError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if c.is_whitespace() {
            i += 1;
            continue;
        }

        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            tokens.push(Token::Ident(chars[start..i].iter().collect()));
            continue;
        }

        if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i < chars.len()
                && chars[i] == '.'
                && chars.get(i + 1).is_some_and(char::is_ascii_digit)
            {
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            tokens.push(Token::Number(chars[start..i].iter().collect()));
            continue;
        }

        if c == '\'' {
            let mut text = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                        text.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        text.push(*ch);
                        i += 1;
                    }
                    None => return Err(ParseError::UnterminatedString),
                }
            }
            tokens.push(Token::String(text));
            continue;
        }

        // Postgres's `<expr>::<type>` cast sugar. Caught here,
        // at the character the user actually typed, so it gets a message
        // naming the two spellings this grammar *does* accept rather than
        // the lexer's generic "unexpected character ':'". See
        // `super::typed_literal` for why `::` isn't one of them.
        if c == ':' {
            return Err(ParseError::UnsupportedCastOperator);
        }

        // In SQL `--` starts a comment, so `a--1` is `a`. This grammar has
        // no comments, and reading the two characters as two minuses would
        // give the same text another meaning (`a - (-1)`), so the pair is
        // refused by name.
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            return Err(ParseError::DoubleMinus);
        }

        // Operators are lexed as runs, exactly as Postgres does, so a
        // two-character comparison exists only when its characters are
        // adjacent (`a<=b`, never `a< =b`) and `a!=-1` is one operator.
        if OP_CHARS.contains(c) {
            let n = operator_run_len(&chars[i..]);
            if n > 1 {
                tokens.push(Token::Operator(chars[i..i + n].iter().collect()));
                i += n;
                continue;
            }
        }

        if SYMBOLS.contains(&c) {
            tokens.push(Token::Symbol(c));
            i += 1;
            continue;
        }

        return Err(ParseError::UnexpectedChar { found: c });
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

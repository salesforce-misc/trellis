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
    Eof,
}

impl Token {
    pub fn describe(&self) -> String {
        match self {
            Token::Ident(s) => format!("identifier '{s}'"),
            Token::Number(s) => format!("number '{s}'"),
            Token::String(s) => format!("string '{s}'"),
            Token::Symbol(c) => format!("'{c}'"),
            Token::Eof => "end of input".to_string(),
        }
    }
}

const SYMBOLS: &[char] = &['+', '-', '*', '/', '%', '=', ',', '(', ')', '.', '<', '>'];

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

        // Postgres's `<expr>::<type>` cast sugar (issue #109). Caught here,
        // at the character the user actually typed, so it gets a message
        // naming the two spellings this grammar *does* accept rather than
        // the lexer's generic "unexpected character ':'". See
        // `super::typed_literal` for why `::` isn't one of them.
        if c == ':' {
            return Err(ParseError::UnsupportedCastOperator);
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

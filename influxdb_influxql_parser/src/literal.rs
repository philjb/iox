#![allow(dead_code)]

use crate::string::{regex, single_quoted_string, Regex};
use crate::write_escaped;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::digit1;
use nom::combinator::{map, map_res, recognize, value};
use nom::multi::fold_many1;
use nom::sequence::{pair, separated_pair};
use nom::IResult;
use std::fmt::{Display, Formatter, Write};

/// Number of nanoseconds in a microsecond.
const NANOS_PER_MICRO: i64 = 1000;
/// Number of nanoseconds in a millisecond.
const NANOS_PER_MILLI: i64 = 1000 * NANOS_PER_MICRO;
/// Number of nanoseconds in a second.
const NANOS_PER_SEC: i64 = 1000 * NANOS_PER_MILLI;
/// Number of nanoseconds in a minute.
const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
/// Number of nanoseconds in an hour.
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MIN;
/// Number of nanoseconds in a day.
const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;
/// Number of nanoseconds in a week.
const NANOS_PER_WEEK: i64 = 7 * NANOS_PER_DAY;

// Primitive InfluxQL literal values, such as strings and regular expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    /// Unsigned integer literal.
    Unsigned(u64),

    /// Float literal.
    Float(f64),

    /// Unescaped string literal.
    String(String),

    /// Boolean literal.
    Boolean(bool),

    /// Duration literal in nanoseconds.
    Duration(Duration),

    /// Unescaped regular expression literal.
    Regex(Regex),
}

impl From<String> for Literal {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<u64> for Literal {
    fn from(v: u64) -> Self {
        Self::Unsigned(v)
    }
}

impl From<f64> for Literal {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for Literal {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<Duration> for Literal {
    fn from(v: Duration) -> Self {
        Self::Duration(v)
    }
}

impl From<Regex> for Literal {
    fn from(v: Regex) -> Self {
        Self::Regex(v)
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsigned(v) => write!(f, "{}", v)?,
            Self::Float(v) => write!(f, "{}", v)?,
            Self::String(v) => {
                f.write_char('\'')?;
                write_escaped!(f, v, '\n' => "\\n", '\\' => "\\\\", '\'' => "\\'", '"' => "\\\"");
                f.write_char('\'')?;
            }
            Self::Boolean(v) => write!(f, "{}", if *v { "true" } else { "false" })?,
            Self::Duration(v) => write!(f, "{}", v)?,
            Self::Regex(v) => write!(f, "{}", v)?,
        }

        Ok(())
    }
}

/// Parse an InfluxQL integer.
///
/// InfluxQL defines an integer as follows
///
/// ```text
/// INTEGER ::= [0-9]+
/// ```
fn integer(i: &str) -> IResult<&str, i64> {
    map_res(digit1, |s: &str| s.parse())(i)
}

/// Parse an unsigned InfluxQL integer.
///
/// InfluxQL defines an integer as follows
///
/// ```text
/// INTEGER ::= [0-9]+
/// ```
fn unsigned_integer(i: &str) -> IResult<&str, u64> {
    map_res(digit1, |s: &str| s.parse())(i)
}

/// Parse an unsigned InfluxQL floating point number.
///
/// InfluxQL defines a floating point number as follows
///
/// ```text
/// float   ::= INTEGER "." INTEGER
/// INTEGER ::= [0-9]+
/// ```
fn float(i: &str) -> IResult<&str, f64> {
    map_res(
        recognize(separated_pair(digit1, tag("."), digit1)),
        |s: &str| s.parse(),
    )(i)
}

/// Parse the input for an InfluxQL boolean, which must be the value `true` or `false`.
fn boolean(i: &str) -> IResult<&str, bool> {
    alt((
        value(true, tag_no_case("true")),
        value(false, tag_no_case("false")),
    ))(i)
}

#[derive(Clone)]
enum DurationUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

/// Represents an InfluxQL duration in nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Duration(i64);

impl From<i64> for Duration {
    fn from(v: i64) -> Self {
        Self(v)
    }
}

static DIVISORS: [(i64, &str); 8] = [
    (NANOS_PER_WEEK, "w"),
    (NANOS_PER_DAY, "d"),
    (NANOS_PER_HOUR, "h"),
    (NANOS_PER_MIN, "m"),
    (NANOS_PER_SEC, "s"),
    (NANOS_PER_MILLI, "ms"),
    (NANOS_PER_MICRO, "us"),
    (1, "ns"),
];

impl Display for Duration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            0 => f.write_str("0s")?,
            mut i => {
                // only return the divisors that are > self
                for (div, unit) in DIVISORS.iter().filter(|(div, _)| self.0 > *div) {
                    let units = i / div;
                    if units > 0 {
                        write!(f, "{}{}", units, unit)?;
                        i -= units * div;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Parse the input for a InfluxQL duration fragment and returns the value in nanoseconds.
fn single_duration(i: &str) -> IResult<&str, i64> {
    use DurationUnit::*;

    map(
        pair(
            integer,
            alt((
                value(Nanosecond, tag("ns")),   // nanoseconds
                value(Microsecond, tag("µs")), // microseconds
                value(Microsecond, tag("us")),  // microseconds
                value(Millisecond, tag("ms")),  // milliseconds
                value(Second, tag("s")),        // seconds
                value(Minute, tag("m")),        // minutes
                value(Hour, tag("h")),          // hours
                value(Day, tag("d")),           // days
                value(Week, tag("w")),          // weeks
            )),
        ),
        |(v, unit)| match unit {
            Nanosecond => v,
            Microsecond => v * NANOS_PER_MICRO,
            Millisecond => v * NANOS_PER_MILLI,
            Second => v * NANOS_PER_SEC,
            Minute => v * NANOS_PER_MIN,
            Hour => v * NANOS_PER_HOUR,
            Day => v * NANOS_PER_DAY,
            Week => v * NANOS_PER_WEEK,
        },
    )(i)
}

/// Parse the input for an InfluxQL duration and returns the value in nanoseconds.
fn duration(i: &str) -> IResult<&str, Duration> {
    map(
        fold_many1(single_duration, || 0, |acc, fragment| acc + fragment),
        Duration,
    )(i)
}

/// Parse an InfluxQL literal, except a [`Regex`].
///
/// See [`literal_regex`] for parsing literal regular expressions.
pub fn literal(i: &str) -> IResult<&str, Literal> {
    alt((
        // NOTE: order is important, as floats should be tested before durations and integers.
        map(float, Literal::Float),
        map(duration, Literal::Duration),
        map(unsigned_integer, Literal::Unsigned),
        map(single_quoted_string, Literal::String),
        map(boolean, Literal::Boolean),
    ))(i)
}

/// Parse an InfluxQL literal regular expression.
pub fn literal_regex(i: &str) -> IResult<&str, Literal> {
    map(regex, Literal::Regex)(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_literal() {
        let (_, got) = literal("42").unwrap();
        assert!(matches!(got, Literal::Unsigned(42)));

        let (_, got) = literal("42.69").unwrap();
        assert!(matches!(got, Literal::Float(v) if v == 42.69));

        let (_, got) = literal("'quick draw'").unwrap();
        assert!(matches!(got, Literal::String(v) if v == "quick draw"));

        let (_, got) = literal("false").unwrap();
        assert!(matches!(got, Literal::Boolean(false)));

        let (_, got) = literal("true").unwrap();
        assert!(matches!(got, Literal::Boolean(true)));

        let (_, got) = literal("3h25m").unwrap();
        assert!(
            matches!(got, Literal::Duration(v) if v == Duration(3 * NANOS_PER_HOUR + 25 * NANOS_PER_MIN))
        );
    }

    #[test]
    fn test_literal_regex() {
        let (_, got) = literal_regex("/^(match|this)$/").unwrap();
        assert!(matches!(got, Literal::Regex(v) if v == "^(match|this)$".into() ));
    }

    #[test]
    fn test_integer() {
        let (_, got) = integer("42").unwrap();
        assert_eq!(got, 42);

        let (_, got) = integer(&i64::MAX.to_string()[..]).unwrap();
        assert_eq!(got, i64::MAX);

        // Fallible cases

        integer("hello").unwrap_err();
    }

    #[test]
    fn test_unsigned_integer() {
        let (_, got) = unsigned_integer("42").unwrap();
        assert_eq!(got, 42);

        let (_, got) = unsigned_integer(&u64::MAX.to_string()[..]).unwrap();
        assert_eq!(got, u64::MAX);

        // Fallible cases

        unsigned_integer("hello").unwrap_err();
    }

    #[test]
    fn test_float() {
        let (_, got) = float("42.69").unwrap();
        assert_eq!(got, 42.69);

        let (_, got) = float(&format!("{:.1}", f64::MAX)[..]).unwrap();
        assert_eq!(got, f64::MAX);

        // Fallible cases

        // missing trailing digits
        float("41.").unwrap_err();

        // missing decimal
        float("41").unwrap_err();
    }

    #[test]
    fn test_boolean() {
        let (_, got) = boolean("true").unwrap();
        assert!(got);
        let (_, got) = boolean("false").unwrap();
        assert!(!got);
    }

    #[test]
    fn test_duration_fragment() {
        let (_, got) = single_duration("38ns").unwrap();
        assert_eq!(got, 38);

        let (_, got) = single_duration("22us").unwrap();
        assert_eq!(got, 22 * NANOS_PER_MICRO);

        let (_, got) = single_duration("7µs").unwrap();
        assert_eq!(got, 7 * NANOS_PER_MICRO);

        let (_, got) = single_duration("15ms").unwrap();
        assert_eq!(got, 15 * NANOS_PER_MILLI);

        let (_, got) = single_duration("53s").unwrap();
        assert_eq!(got, 53 * NANOS_PER_SEC);

        let (_, got) = single_duration("158m").unwrap();
        assert_eq!(got, 158 * NANOS_PER_MIN);

        let (_, got) = single_duration("39h").unwrap();
        assert_eq!(got, 39 * NANOS_PER_HOUR);

        let (_, got) = single_duration("2d").unwrap();
        assert_eq!(got, 2 * NANOS_PER_DAY);

        let (_, got) = single_duration("5w").unwrap();
        assert_eq!(got, 5 * NANOS_PER_WEEK);
    }

    #[test]
    fn test_duration() {
        let (_, got) = duration("10h3m2s").unwrap();
        assert_eq!(
            got,
            Duration(10 * NANOS_PER_HOUR + 3 * NANOS_PER_MIN + 2 * NANOS_PER_SEC)
        );
    }

    #[test]
    fn test_display_duration() {
        let (_, d) = duration("3w2h15ms").unwrap();
        let got = format!("{}", d);
        assert_eq!(got, "3w2h15ms");

        let (_, d) = duration("5s5s5s5s5s").unwrap();
        let got = format!("{}", d);
        assert_eq!(got, "25s");

        let d = Duration(0);
        let got = format!("{}", d);
        assert_eq!(got, "0s");

        let d = Duration(
            20 * NANOS_PER_WEEK
                + 6 * NANOS_PER_DAY
                + 13 * NANOS_PER_HOUR
                + 11 * NANOS_PER_MIN
                + 10 * NANOS_PER_SEC
                + 9 * NANOS_PER_MILLI
                + 8 * NANOS_PER_MICRO
                + 500,
        );
        let got = format!("{}", d);
        assert_eq!(got, "20w6d13h11m10s9ms8us500ns");
    }
}
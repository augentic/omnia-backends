//! Translation of a [`Filter`] into an Azure Table `OData` `$filter` string.
//!
//! Azure Table's `OData` query layer supports comparison operators and the
//! logical combinators `and`, `or`, and `not`, so an `in` expands to an `or`
//! chain. It does **not** support string functions (`contains`,
//! `startswith`, `endswith`), null-checks, or nested fields — see
//! <https://learn.microsoft.com/en-us/rest/api/storageservices/querying-tables-and-entities#supported-query-options>.
//!
//! Filters that cannot be evaluated server-side fail with
//! [`Error::Unsupported`] rather than silently falling back to client-side
//! evaluation, which could pull unbounded data from the table service.

use anyhow::bail;
use omnia_wasi_docstore::{Condition, Error, Filter, Test, Value};

/// Translate a [`Filter`] to an `OData` `$filter` string, `None` when it selects every entity.
///
/// Each group is an `or` of its conditions in parentheses, and the groups
/// join with `and`. A negated condition is a `not`.
///
/// # Errors
///
/// - [`Error::Unsupported`] for a condition Azure Table cannot evaluate
///   server-side, anywhere in the filter: a [`Test::Null`],
///   [`Test::StartsWith`], [`Test::EndsWith`], [`Test::Contains`], or
///   [`Test::ArrayContains`], or a dotted field path.
/// - [`Error::Invalid`] for any other field that is not an identifier.
pub fn to_odata(filter: &Filter) -> anyhow::Result<Option<String>> {
    let groups = filter
        .iter()
        .map(|group| {
            let conditions = group.iter().map(condition).collect::<anyhow::Result<Vec<_>>>()?;
            Ok(format!("({})", conditions.join(" or ")))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((!groups.is_empty()).then(|| groups.join(" and ")))
}

fn condition(condition: &Condition) -> anyhow::Result<String> {
    let Condition { field, test, negated } = condition;
    validate_field(field)?;
    let compare = |op: &str, value: &Value| format!("{field} {op} {}", odata_value(value));

    let test = match test {
        Test::Eq(value) => compare("eq", value),
        Test::Lt(value) => compare("lt", value),
        Test::Lte(value) => compare("le", value),
        Test::Gt(value) => compare("gt", value),
        Test::Gte(value) => compare("ge", value),
        Test::In(values) => {
            let equals: Vec<String> = values.iter().map(|value| compare("eq", value)).collect();
            format!("({})", equals.join(" or "))
        }
        Test::Null => {
            bail!(Error::Unsupported(format!(
                "a null test of '{field}' is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )))
        }
        Test::StartsWith(_) | Test::EndsWith(_) | Test::Contains(_) => {
            bail!(Error::Unsupported(format!(
                "a text test of '{field}' is not supported by Azure Table — OData $filter does not support string functions"
            )))
        }
        Test::ArrayContains(_) => {
            bail!(Error::Unsupported(format!(
                "an array test of '{field}' is not supported by Azure Table — arrays are stored as JSON strings"
            )))
        }
    };
    Ok(if *negated { format!("not ({test})") } else { test })
}

// A property name is emitted unquoted, so it must be a bare identifier. A
// dotted path of identifiers names a nested field, which the table holds as
// a JSON string it cannot filter into.
fn validate_field(field: &str) -> anyhow::Result<()> {
    if is_identifier(field) {
        return Ok(());
    }
    if field.split('.').all(is_identifier) {
        bail!(Error::Unsupported(format!(
            "dotted path '{field}' is not supported by Azure Table — nested values are stored as JSON strings"
        )));
    }
    bail!(Error::Invalid(format!(
        "invalid OData property name: '{field}' — must match [A-Za-z_][A-Za-z0-9_]*"
    )))
}

fn is_identifier(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// `document` stores an integer that fits in 32 bits as an `Edm.Int32`, so a
// literal that fits is written as one too.
fn odata_value(value: &Value) -> String {
    match value {
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i32::try_from(*i).map_or_else(|_| format!("{i}L"), |i| i.to_string()),
        Value::Number(f) => {
            if f.fract() == 0.0 {
                format!("{f:.1}")
            } else {
                format!("{f}")
            }
        }
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

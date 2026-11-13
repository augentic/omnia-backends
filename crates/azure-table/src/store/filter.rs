//! Translation of [`FilterTree`] into Azure Table `OData` `$filter` strings.
//!
//! Azure Table's `OData` query layer supports comparison operators, logical
//! combinators (`and`, `or`, `not`), and set membership (`InList` / `NotInList`
//! expanded to OR chains). It does **not** support string functions
//! (`contains`, `startswith`, `endswith`), null-checks, or nested fields — see
//! <https://learn.microsoft.com/en-us/rest/api/storageservices/querying-tables-and-entities#supported-query-options>.
//!
//! Filters that cannot be evaluated server-side fail with
//! [`Error::Unsupported`] rather than silently falling back to client-side
//! evaluation, which could pull unbounded data from the table service.

use std::fmt::Write;

use anyhow::bail;
use omnia_wasi_docstore::{ComparisonOp, Error, FilterTree, ScalarValue};

/// Translate a [`FilterTree`] to an `OData` `$filter` string.
///
/// # Errors
///
/// - [`Error::Unsupported`] for a node Azure Table cannot evaluate
///   server-side, anywhere in the tree: `IsNull`, `IsNotNull`, `Contains`,
///   `StartsWith`, `EndsWith`, a comparison with `Null`, or a dotted field
///   path.
/// - [`Error::Invalid`] for any other field that is not an identifier.
/// - An untyped error for an empty `InList`, `NotInList`, `And`, or `Or`.
pub fn to_odata(filter: &FilterTree) -> anyhow::Result<String> {
    match filter {
        FilterTree::Compare { field, op, value } => {
            validate_field(field)?;
            Ok(format!("{field} {} {}", odata_op(*op), odata_value(value)?))
        }
        FilterTree::InList { field, values } => {
            validate_field(field)?;
            if values.is_empty() {
                bail!("InList('{field}') requires at least one value");
            }
            let parts: Vec<String> = values
                .iter()
                .map(|v| Ok(format!("{field} eq {}", odata_value(v)?)))
                .collect::<anyhow::Result<_>>()?;
            Ok(format!("({})", parts.join(" or ")))
        }
        FilterTree::NotInList { field, values } => {
            validate_field(field)?;
            if values.is_empty() {
                bail!("NotInList('{field}') requires at least one value");
            }
            let parts: Vec<String> = values
                .iter()
                .map(|v| Ok(format!("{field} eq {}", odata_value(v)?)))
                .collect::<anyhow::Result<_>>()?;
            Ok(format!("not ({})", parts.join(" or ")))
        }
        FilterTree::And(children) => {
            if children.is_empty() {
                bail!("And requires at least one child filter");
            }
            let parts: Vec<String> =
                children.iter().map(to_odata).collect::<anyhow::Result<_>>()?;
            Ok(parts.join(" and "))
        }
        FilterTree::Or(children) => {
            if children.is_empty() {
                bail!("Or requires at least one child filter");
            }
            let parts: Vec<String> =
                children.iter().map(to_odata).collect::<anyhow::Result<_>>()?;
            Ok(format!("({})", parts.join(" or ")))
        }
        FilterTree::Not(inner) => Ok(format!("not ({})", to_odata(inner)?)),
        FilterTree::IsNull(field) => {
            bail!(Error::Unsupported(format!(
                "IsNull('{field}') is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )))
        }
        FilterTree::IsNotNull(field) => {
            bail!(Error::Unsupported(format!(
                "IsNotNull('{field}') is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )))
        }
        FilterTree::Contains { field, .. } => {
            bail!(Error::Unsupported(format!(
                "Contains('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )))
        }
        FilterTree::StartsWith { field, .. } => {
            bail!(Error::Unsupported(format!(
                "StartsWith('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )))
        }
        FilterTree::EndsWith { field, .. } => {
            bail!(Error::Unsupported(format!(
                "EndsWith('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )))
        }
    }
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

const fn odata_op(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "eq",
        ComparisonOp::Ne => "ne",
        ComparisonOp::Gt => "gt",
        ComparisonOp::Gte => "ge",
        ComparisonOp::Lt => "lt",
        ComparisonOp::Lte => "le",
    }
}

fn odata_value(v: &ScalarValue) -> anyhow::Result<String> {
    Ok(match v {
        ScalarValue::Null => {
            bail!(Error::Unsupported(
                "null comparison is not supported by Azure Table — \
                 properties that are null are omitted from entities entirely"
                    .to_owned()
            ))
        }
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int32(i) => i.to_string(),
        ScalarValue::Int64(i) => format!("{i}L"),
        ScalarValue::Float64(f) => {
            if f.fract() == 0.0 {
                format!("{f:.1}")
            } else {
                format!("{f}")
            }
        }
        ScalarValue::Str(s) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Binary(b) => {
            let hex = b.iter().fold(String::with_capacity(b.len() * 2), |mut acc, byte| {
                let _ = write!(acc, "{byte:02x}");
                acc
            });
            format!("X'{hex}'")
        }
        ScalarValue::Timestamp(t) => format!("datetime'{}'", t.replace('\'', "''")),
    })
}

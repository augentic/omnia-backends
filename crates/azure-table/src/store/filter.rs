//! Translation of [`FilterTree`] into Azure Table `OData` `$filter` strings.
//!
//! Azure Table's `OData` query layer supports comparison operators, logical
//! combinators (`and`, `or`, `not`), and set membership (`InList` / `NotInList`
//! expanded to OR chains). It does **not** support string functions
//! (`contains`, `startswith`, `endswith`) or null-checks — see
//! <https://learn.microsoft.com/en-us/rest/api/storageservices/querying-tables-and-entities#supported-query-options>.
//!
//! Filters that cannot be evaluated server-side are rejected with an error
//! rather than silently falling back to client-side evaluation, which could
//! pull unbounded data from the table service.

use std::fmt::Write;

use anyhow::bail;
use omnia_wasi_docstore::{ComparisonOp, FilterTree, ScalarValue};

// A property name is emitted unquoted, so it must be a bare identifier.
fn validate_field(field: &str) -> anyhow::Result<()> {
    if field.is_empty()
        || !field.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        || !field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        bail!("invalid OData property name: '{field}' — must match [A-Za-z_][A-Za-z0-9_]*");
    }
    Ok(())
}

/// Translate a [`FilterTree`] to an `OData` `$filter` string.
///
/// # Errors
///
/// Returns an error if the filter contains nodes that Azure Table cannot
/// evaluate server-side: `IsNull`, `IsNotNull`, `Contains`, `StartsWith`,
/// `EndsWith`, or any logical combinator (`And`, `Or`, `Not`) whose children
/// include such nodes.
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
            bail!(
                "IsNull('{field}') is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )
        }
        FilterTree::IsNotNull(field) => {
            bail!(
                "IsNotNull('{field}') is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )
        }
        FilterTree::Contains { field, .. } => {
            bail!(
                "Contains('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )
        }
        FilterTree::StartsWith { field, .. } => {
            bail!(
                "StartsWith('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )
        }
        FilterTree::EndsWith { field, .. } => {
            bail!(
                "EndsWith('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )
        }
    }
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
            bail!(
                "null comparison is not supported by Azure Table — \
                 use IsNull/IsNotNull filter nodes (which are also unsupported) \
                 or omit the filter for absent properties"
            )
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

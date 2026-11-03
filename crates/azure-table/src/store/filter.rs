//! Translation of [`Filter`] into Azure Table `OData` `$filter` strings.
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
use omnia_backend::docstore::{ComparisonOp, Filter, ScalarValue};

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

/// Translate a [`Filter`] to an `OData` `$filter` string.
///
/// # Errors
///
/// Returns an error if the filter contains nodes that Azure Table cannot
/// evaluate server-side: `IsNull`, `IsNotNull`, `Contains`, `StartsWith`,
/// `EndsWith`, or any logical combinator (`And`, `Or`, `Not`) whose children
/// include such nodes.
pub fn to_odata(filter: &Filter) -> anyhow::Result<String> {
    match filter {
        Filter::Compare { field, op, value } => {
            validate_field(field)?;
            Ok(format!("{field} {} {}", odata_op(*op), odata_value(value)?))
        }
        Filter::InList { field, values } => {
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
        Filter::NotInList { field, values } => {
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
        Filter::And(children) => {
            if children.is_empty() {
                bail!("And requires at least one child filter");
            }
            let parts: Vec<String> =
                children.iter().map(to_odata).collect::<anyhow::Result<_>>()?;
            Ok(parts.join(" and "))
        }
        Filter::Or(children) => {
            if children.is_empty() {
                bail!("Or requires at least one child filter");
            }
            let parts: Vec<String> =
                children.iter().map(to_odata).collect::<anyhow::Result<_>>()?;
            Ok(format!("({})", parts.join(" or ")))
        }
        Filter::Not(inner) => Ok(format!("not ({})", to_odata(inner)?)),
        Filter::IsNull(field) => {
            bail!(
                "IsNull('{field}') is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )
        }
        Filter::IsNotNull(field) => {
            bail!(
                "IsNotNull('{field}') is not supported by Azure Table — properties that are null are omitted from entities entirely"
            )
        }
        Filter::Contains { field, .. } => {
            bail!(
                "Contains('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )
        }
        Filter::StartsWith { field, .. } => {
            bail!(
                "StartsWith('{field}') is not supported by Azure Table — OData $filter does not support string functions"
            )
        }
        Filter::EndsWith { field, .. } => {
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
        ScalarValue::Bool(b) => b.to_string(),
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

// OData `$filter` translation and unsupported-node rejection; the service
// evaluating the generated filter is `tests/live.rs::query_and_pagination`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_eq() {
        let f = Filter::Compare {
            field: "Name".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Str("Alice".into()),
        };
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "Name eq 'Alice'");
    }

    #[test]
    fn in_list() {
        let f = Filter::InList {
            field: "Status".into(),
            values: vec![ScalarValue::Int32(1), ScalarValue::Int32(2)],
        };
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "(Status eq 1 or Status eq 2)");
    }

    #[test]
    fn is_null() {
        let f = Filter::IsNull("Zone".into());
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn contains() {
        let f = Filter::Contains {
            field: "Name".into(),
            pattern: "Alice".into(),
        };
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn starts_with() {
        let f = Filter::StartsWith {
            field: "Name".into(),
            pattern: "Al".into(),
        };
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn ends_with() {
        let f = Filter::EndsWith {
            field: "Name".into(),
            pattern: "ce".into(),
        };
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn is_not_null() {
        let f = Filter::IsNotNull("Zone".into());
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn and_unsupported_child() {
        let f = Filter::And(vec![
            Filter::Compare {
                field: "Active".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Bool(true),
            },
            Filter::Contains {
                field: "Name".into(),
                pattern: "Alice".into(),
            },
        ]);
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn timestamp() {
        let f = Filter::Compare {
            field: "Created".into(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Timestamp("2026-01-01T00:00:00Z".into()),
        };
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "Created ge datetime'2026-01-01T00:00:00Z'");
    }

    #[test]
    fn not_in_list() {
        let f = Filter::NotInList {
            field: "Name".into(),
            values: vec![ScalarValue::Str("Alice".into()), ScalarValue::Str("Bob".into())],
        };
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "not (Name eq 'Alice' or Name eq 'Bob')");
    }

    #[test]
    fn not_compare() {
        let f = Filter::Not(Box::new(Filter::Compare {
            field: "IsActive".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Bool(true),
        }));
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "not (IsActive eq true)");
    }

    #[test]
    fn field_injection() {
        let f = Filter::Compare {
            field: "RowKey eq 'admin' or 1 eq 1 and Name".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Str("x".into()),
        };
        let err = to_odata(&f).unwrap_err().to_string();
        assert!(err.contains("invalid OData property name"), "{err}");
    }

    #[test]
    fn empty_field() {
        let f = Filter::Compare {
            field: String::new(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Int32(1),
        };
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn field_underscore() {
        let f = Filter::Compare {
            field: "_internal_flag".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Bool(true),
        };
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "_internal_flag eq true");
    }

    #[test]
    fn null_comparison() {
        let f = Filter::Compare {
            field: "Region".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Null,
        };
        let err = to_odata(&f).unwrap_err().to_string();
        assert!(err.contains("null comparison is not supported"), "{err}");
    }

    #[test]
    fn empty_in_list() {
        let f = Filter::InList {
            field: "Status".into(),
            values: vec![],
        };
        let err = to_odata(&f).unwrap_err().to_string();
        assert!(err.contains("requires at least one value"), "{err}");
    }

    #[test]
    fn empty_not_in_list() {
        let f = Filter::NotInList {
            field: "Status".into(),
            values: vec![],
        };
        to_odata(&f).unwrap_err();
    }

    #[test]
    fn float_whole_number() {
        let f = Filter::Compare {
            field: "Rating".into(),
            op: ComparisonOp::Gt,
            value: ScalarValue::Float64(3.0),
        };
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "Rating gt 3.0");
    }

    #[test]
    fn timestamp_single_quote() {
        let f = Filter::Compare {
            field: "Created".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Timestamp("2026-01-01T00:00:00Z'injected".into()),
        };
        let odata = to_odata(&f).unwrap();
        assert!(odata.contains("''injected"), "single quote should be doubled: {odata}");
    }

    #[test]
    fn or() {
        let f = Filter::Or(vec![
            Filter::Compare {
                field: "Points".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Int32(200),
            },
            Filter::Compare {
                field: "Points".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Int32(150),
            },
        ]);
        let odata = to_odata(&f).unwrap();
        assert_eq!(odata, "(Points eq 200 or Points eq 150)");
    }

    #[test]
    fn empty_and() {
        let f = Filter::And(vec![]);
        let err = to_odata(&f).unwrap_err().to_string();
        assert!(err.contains("at least one child"), "{err}");
    }

    #[test]
    fn empty_or() {
        let f = Filter::Or(vec![]);
        let err = to_odata(&f).unwrap_err().to_string();
        assert!(err.contains("at least one child"), "{err}");
    }
}

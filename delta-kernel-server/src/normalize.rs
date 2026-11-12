//! Canonical encoding of plan proto bytes (SPEC §8 "辅助判据").
//!
//! prost encodes proto `map<>` fields from a `HashMap`, so two encodings of the same kernel plan
//! can differ in byte order (`StructField.metadata`, `Transform.field_transforms`). Kernel's
//! `MapData` pairs may also come from a `HashMap`. Normalization decodes the bytes into a
//! BTreeMap-backed copy of kernel's protos and sorts every `MapData`'s pairs by the encoded key,
//! then re-encodes. Two plans are "normalized-equal" iff these bytes are equal.

use prost::Message as _;

/// BTreeMap-backed, wire-compatible copies of `kernel/proto/*.proto`.
#[allow(clippy::all, missing_docs)]
pub mod canonical {
    pub mod schema {
        include!(concat!(
            env!("OUT_DIR"),
            "/normalized/delta.kernel.schema.rs"
        ));
    }
    pub mod expressions {
        include!(concat!(
            env!("OUT_DIR"),
            "/normalized/delta.kernel.expressions.rs"
        ));
    }
    pub mod plan {
        include!(concat!(env!("OUT_DIR"), "/normalized/delta.kernel.plan.rs"));
    }
    #[allow(clippy::module_inception)]
    pub mod operation {
        include!(concat!(
            env!("OUT_DIR"),
            "/normalized/delta.kernel.operation.rs"
        ));
    }
}

use canonical::{expressions as e, plan as p};

/// Returns the canonical encoding of `delta.kernel.plan.Plan` bytes.
pub fn normalize_plan_bytes(bytes: &[u8]) -> Result<Vec<u8>, prost::DecodeError> {
    let mut plan = p::Plan::decode(bytes)?;
    for node in &mut plan.nodes {
        if let Some(op) = node.op.as_mut().and_then(|o| o.op.as_mut()) {
            operator(op);
        }
    }
    Ok(plan.encode_to_vec())
}

fn operator(op: &mut p::operator::Op) {
    use p::operator::Op;
    match op {
        Op::ScanParquet(n) => n.files.iter_mut().for_each(scan_file),
        Op::ScanJson(n) => n.files.iter_mut().for_each(scan_file),
        Op::Values(n) => n
            .rows
            .iter_mut()
            .flat_map(|r| r.values.iter_mut())
            .for_each(scalar),
        Op::Project(n) => {
            if let Some(x) = n.expr.as_mut() {
                expression(x)
            }
        }
        Op::Filter(n) => {
            if let Some(x) = n.predicate.as_mut() {
                predicate(x)
            }
        }
        Op::RelationSource(_)
        | Op::UnionAll(_)
        | Op::DynamicScan(_)
        | Op::Aggregate(_)
        | Op::SemiJoin(_)
        | Op::PrefixSum(_)
        | Op::WriteJson(_) => {}
    }
}

fn scan_file(f: &mut p::ScanFile) {
    f.file_constants.iter_mut().for_each(scalar);
}

fn expression(x: &mut e::Expression) {
    use e::expression::Kind;
    let Some(kind) = x.kind.as_mut() else { return };
    match kind {
        Kind::Literal(s) => scalar(s),
        Kind::Predicate(pr) => predicate(pr),
        Kind::StructExpr(s) => {
            s.exprs.iter_mut().for_each(expression);
            if let Some(n) = s.nullability_predicate.as_mut() {
                expression(n);
            }
        }
        Kind::Transform(t) => {
            t.field_transforms
                .values_mut()
                .flat_map(|ft| ft.exprs.iter_mut())
                .for_each(expression);
            t.prepended_fields.iter_mut().for_each(expression);
            t.appended_fields.iter_mut().for_each(expression);
        }
        Kind::Unary(u) => {
            if let Some(x) = u.expr.as_mut() {
                expression(x)
            }
        }
        Kind::Binary(b) => {
            if let Some(x) = b.left.as_mut() {
                expression(x)
            }
            if let Some(x) = b.right.as_mut() {
                expression(x)
            }
        }
        Kind::Variadic(v) => v.exprs.iter_mut().for_each(expression),
        Kind::CaseWhen(c) => {
            for b in &mut c.branches {
                if let Some(pr) = b.condition.as_mut() {
                    predicate(pr);
                }
                if let Some(x) = b.result.as_mut() {
                    expression(x);
                }
            }
            if let Some(x) = c.else_expr.as_mut() {
                expression(x);
            }
        }
        Kind::Opaque(o) => o.exprs.iter_mut().for_each(expression),
        Kind::ParseJson(pj) => {
            if let Some(x) = pj.json_expr.as_mut() {
                expression(x)
            }
        }
        Kind::MapToStruct(m) => {
            if let Some(x) = m.map_expr.as_mut() {
                expression(x)
            }
        }
        Kind::Column(_) | Kind::Unknown(_) => {}
    }
}

fn predicate(x: &mut e::Predicate) {
    use e::predicate::Kind;
    let Some(kind) = x.kind.as_mut() else { return };
    match kind {
        Kind::BooleanExpression(b) => expression(b),
        Kind::Not(n) => predicate(n),
        Kind::Unary(u) => {
            if let Some(x) = u.expr.as_mut() {
                expression(x)
            }
        }
        Kind::Binary(b) => {
            if let Some(x) = b.left.as_mut() {
                expression(x)
            }
            if let Some(x) = b.right.as_mut() {
                expression(x)
            }
        }
        Kind::Junction(j) => j.preds.iter_mut().for_each(predicate),
        Kind::Opaque(o) => o.exprs.iter_mut().for_each(expression),
        Kind::Unknown(_) => {}
    }
}

fn scalar(s: &mut e::Scalar) {
    use e::scalar::Value;
    let Some(v) = s.value.as_mut() else { return };
    match v {
        Value::Struct(sd) => sd.values.iter_mut().for_each(scalar),
        Value::Array(a) => a.elements.iter_mut().for_each(scalar),
        Value::Map(m) => {
            for pair in &mut m.pairs {
                if let Some(k) = pair.key.as_mut() {
                    scalar(k);
                }
                if let Some(v) = pair.value.as_mut() {
                    scalar(v);
                }
            }
            m.pairs
                .sort_by_cached_key(|pair| pair.key.as_ref().map(|k| k.encode_to_vec()));
        }
        _ => {}
    }
}

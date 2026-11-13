//! Decodes kernel `Operation` proto bytes back into the kernel plan IR.
//!
//! Kernel only ships the IR -> proto direction (plus `StructType` back). The loopback test client
//! needs the inverse to execute the ops the server forwards with an in-process executor, and L0b
//! replay (SPEC §8) needs it to match recorded ops semantically. Opaque expressions/predicates
//! and `Unknown` placeholders (which is how `Cast` is encoded) cannot be reconstructed and are
//! rejected; metadata scan plans without a predicate do not contain them.

use std::sync::Arc;

use delta_kernel::expressions::{
    ArrayData, BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp,
    CaseWhenExpression, ColumnName, DecimalData, Expression, ExpressionRef, JunctionPredicate,
    JunctionPredicateOp, MapData, MapToStructExpression, MapToStructOptions, ParseJsonExpression,
    Predicate, Scalar, StructData, UnaryExpression, UnaryExpressionOp, UnaryPredicate,
    UnaryPredicateOp, VariadicExpression, VariadicExpressionOp,
};
use delta_kernel::plans::ir::nodes::{
    Agg, Aggregate, DynamicScan, FileType, Filter, NonNullByOperands, Operator, PrefixSum, Project,
    ScanFile, ScanJson, ScanParquet, SemiJoin, UnionAll, Values, WriteJson,
};
use delta_kernel::plans::ir::plan::{Plan, PlanNode};
use delta_kernel::plans::proto::{expressions as pe, operation as po, plan as pp, schema as ps};
use delta_kernel::schema::{
    ArrayType, DataType, DecimalType, IntervalYearToMonthType, MapType, SchemaRef, StructField,
    StructType,
};
use delta_kernel::struct_patch::{ExpressionFieldPatch, ExpressionStructPatch};
use delta_kernel::{FileMeta, FileSlice, IoOperation, KernelError, KernelResult, Operation};
use prost::Message as _;
use url::Url;

fn missing(what: &str) -> KernelError {
    KernelError::generic(format!("decode: missing {what}"))
}

/// Decodes `delta.kernel.operation.Operation` bytes.
pub fn decode_operation(bytes: &[u8]) -> KernelResult<Operation> {
    let op = po::Operation::decode(bytes).map_err(KernelError::generic_err)?;
    operation(op)
}

/// Decodes `delta.kernel.plan.Plan` bytes (as returned in `ScanPlanResult.plan`).
pub fn decode_plan(bytes: &[u8]) -> KernelResult<Plan> {
    plan(pp::Plan::decode(bytes).map_err(KernelError::generic_err)?)
}

fn operation(op: po::Operation) -> KernelResult<Operation> {
    use po::operation::Op;
    match op.op.ok_or_else(|| missing("Operation.op"))? {
        Op::Io(io) => Ok(Operation::IoOperation(io_operation(io)?)),
        Op::QueryPlan(p) => Ok(Operation::QueryPlan(plan(p)?)),
    }
}

fn url(s: &str) -> KernelResult<Url> {
    Ok(Url::parse(s)?)
}

fn io_operation(io: po::IoOperation) -> KernelResult<IoOperation> {
    use po::io_operation::Op;
    Ok(match io.op.ok_or_else(|| missing("IoOperation.op"))? {
        Op::FileListing(f) => IoOperation::file_listing(url(&f.url)?),
        Op::ReadBytes(r) => IoOperation::read_bytes(
            r.files
                .into_iter()
                .map(file_slice)
                .collect::<KernelResult<_>>()?,
        ),
        Op::WriteBytes(w) => {
            IoOperation::write_bytes(url(&w.url)?, bytes::Bytes::from(w.data), w.overwrite)
        }
        Op::HeadFile(h) => IoOperation::head_file(url(&h.url)?),
        Op::AtomicCopy(c) => IoOperation::atomic_copy(url(&c.source)?, url(&c.destination)?),
        Op::ParquetFooter(f) => {
            IoOperation::parquet_footer(file_meta(f.file.ok_or_else(|| missing("file"))?)?)
        }
    })
}

fn file_slice(s: po::FileSlice) -> KernelResult<FileSlice> {
    let range = match (s.range_start, s.range_end) {
        (Some(start), Some(end)) => Some(start..end),
        (None, None) => None,
        _ => {
            return Err(KernelError::generic(
                "decode: half-specified FileSlice range",
            ));
        }
    };
    Ok((url(&s.url)?, range))
}

fn file_meta(m: pp::FileMeta) -> KernelResult<FileMeta> {
    Ok(FileMeta {
        location: url(&m.location)?,
        last_modified: m.last_modified,
        size: m.size,
    })
}

fn schema(s: Option<ps::StructType>) -> KernelResult<SchemaRef> {
    Ok(Arc::new(StructType::try_from(
        s.ok_or_else(|| missing("schema"))?,
    )?))
}

fn column(c: Option<pe::ColumnName>) -> KernelResult<ColumnName> {
    Ok(ColumnName::new(c.ok_or_else(|| missing("column"))?.path))
}

fn columns(c: Vec<pe::ColumnName>) -> Vec<ColumnName> {
    c.into_iter().map(|c| ColumnName::new(c.path)).collect()
}

/// Decodes a plan, reconstructing `Plan.schema` (which the proto drops) from the terminal node.
fn plan(p: pp::Plan) -> KernelResult<Plan> {
    let mut nodes: Vec<PlanNode> = Vec::with_capacity(p.nodes.len());
    let mut schemas: Vec<SchemaRef> = Vec::with_capacity(p.nodes.len());
    for (idx, node) in p.nodes.into_iter().enumerate() {
        let inputs: Vec<usize> = node.inputs.iter().map(|&i| i as usize).collect();
        if let Some(bad) = inputs.iter().find(|&&i| i >= idx) {
            return Err(KernelError::generic(format!(
                "decode: node {idx} input {bad} is not topologically earlier"
            )));
        }
        let op = operator(node.op.ok_or_else(|| missing("PlanNode.op"))?)?;
        let first_input = || {
            inputs
                .first()
                .map(|&i| schemas[i].clone())
                .ok_or_else(|| missing("operator input"))
        };
        let out_schema = match &op {
            Operator::ScanParquet(n) => n.schema.clone(),
            Operator::ScanJson(n) => n.schema.clone(),
            Operator::Values(n) => n.schema.clone(),
            Operator::RelationSource(r) => r.schema().clone(),
            Operator::Project(n) => n.schema.clone(),
            Operator::DynamicScan(n) => n.schema.clone(),
            Operator::Aggregate(n) => n.schema.clone(),
            Operator::PrefixSum(n) => n.schema.clone(),
            Operator::WriteJson(_) => delta_kernel::plans::ir::nodes::FILE_META_SCHEMA.clone(),
            Operator::Filter(_) | Operator::SemiJoin(_) | Operator::UnionAll(_) => first_input()?,
        };
        schemas.push(out_schema);
        nodes.push(PlanNode { op, inputs });
    }
    let schema = schemas
        .last()
        .cloned()
        .ok_or_else(|| missing("plan nodes"))?;
    Ok(Plan { schema, nodes })
}

fn scan_files(files: Vec<pp::ScanFile>) -> KernelResult<Vec<ScanFile>> {
    files
        .into_iter()
        .map(|f| {
            Ok(ScanFile {
                meta: file_meta(f.meta.ok_or_else(|| missing("ScanFile.meta"))?)?,
                file_constants: f
                    .file_constants
                    .into_iter()
                    .map(scalar)
                    .collect::<KernelResult<_>>()?,
            })
        })
        .collect()
}

fn operator(op: pp::Operator) -> KernelResult<Operator> {
    use pp::operator::Op;
    Ok(match op.op.ok_or_else(|| missing("Operator.op"))? {
        Op::ScanParquet(n) => Operator::ScanParquet(ScanParquet {
            files: scan_files(n.files)?,
            file_constant_columns: n.file_constant_columns,
            schema: schema(n.schema)?,
        }),
        Op::ScanJson(n) => Operator::ScanJson(ScanJson {
            files: scan_files(n.files)?,
            file_constant_columns: n.file_constant_columns,
            schema: schema(n.schema)?,
        }),
        Op::Values(n) => Operator::Values(Values {
            schema: schema(n.schema)?,
            rows: n
                .rows
                .into_iter()
                .map(|r| r.values.into_iter().map(scalar).collect())
                .collect::<KernelResult<_>>()?,
        }),
        Op::RelationSource(_) => {
            return Err(KernelError::unsupported(
                "decode: RelationSource refs are executor-scoped and cannot cross processes",
            ));
        }
        Op::Project(n) => Operator::Project(Project {
            expr: Arc::new(expression(n.expr.ok_or_else(|| missing("Project.expr"))?)?),
            schema: schema(n.schema)?,
        }),
        Op::Filter(n) => Operator::Filter(Filter {
            predicate: Arc::new(predicate(
                n.predicate.ok_or_else(|| missing("Filter.predicate"))?,
            )?),
        }),
        Op::UnionAll(_) => Operator::UnionAll(UnionAll),
        Op::DynamicScan(n) => Operator::DynamicScan(DynamicScan {
            schema: schema(n.schema)?,
            file_type: match pp::FileType::try_from(n.file_type) {
                Ok(pp::FileType::Parquet) => FileType::Parquet,
                Ok(pp::FileType::Json) => FileType::Json,
                _ => return Err(KernelError::generic("decode: bad DynamicScan.file_type")),
            },
            base_url: url(&n.base_url)?,
            file_constant_columns: n.file_constant_columns,
            path_column: column(n.path_column)?,
            file_size_column: column(n.file_size_column)?,
            last_modified_column: column(n.last_modified_column)?,
            dv_column: n.dv_column.map(|c| ColumnName::new(c.path)),
        }),
        Op::Aggregate(n) => Operator::Aggregate(Aggregate {
            group_by: columns(n.group_by),
            aggs: n.aggs.into_iter().map(agg).collect::<KernelResult<_>>()?,
            schema: schema(n.schema)?,
        }),
        Op::SemiJoin(n) => Operator::SemiJoin(SemiJoin {
            inverted: n.inverted,
            probe_keys: columns(n.probe_keys),
            build_keys: columns(n.build_keys),
        }),
        Op::PrefixSum(n) => Operator::PrefixSum(PrefixSum {
            input: column(n.input)?,
            output: n.output,
            schema: schema(n.schema)?,
        }),
        Op::WriteJson(n) => Operator::WriteJson(WriteJson {
            schema: schema(n.schema)?,
            file_path: url(&n.file_path)?,
            overwrite: n.overwrite,
            ordered: n.ordered,
        }),
    })
}

fn agg(a: pp::Agg) -> KernelResult<Agg> {
    use pp::agg::Func;
    let by = |value, null_sentinel, key| -> KernelResult<NonNullByOperands> {
        Ok(NonNullByOperands {
            value: column(value)?,
            null_sentinel: column(null_sentinel)?,
            key: column(key)?,
        })
    };
    Ok(match a.func.ok_or_else(|| missing("Agg.func"))? {
        Func::Min(m) => Agg::Min(column(m.value)?),
        Func::Max(m) => Agg::Max(column(m.value)?),
        Func::Sum(m) => Agg::Sum(column(m.value)?),
        Func::Count(m) => Agg::Count(column(m.value)?),
        Func::CountStar(_) => Agg::CountStar,
        Func::MinNonNullBy(m) => Agg::MinNonNullBy(by(m.value, m.null_sentinel, m.key)?),
        Func::MaxNonNullBy(m) => Agg::MaxNonNullBy(by(m.value, m.null_sentinel, m.key)?),
    })
}

fn boxed(e: Option<Box<pe::Expression>>, what: &str) -> KernelResult<Box<Expression>> {
    Ok(Box::new(expression(*e.ok_or_else(|| missing(what))?)?))
}

fn expr_refs(v: Vec<pe::Expression>) -> KernelResult<Vec<ExpressionRef>> {
    v.into_iter()
        .map(|e| Ok(Arc::new(expression(e)?)))
        .collect()
}

fn expression(e: pe::Expression) -> KernelResult<Expression> {
    use pe::expression::Kind;
    Ok(match e.kind.ok_or_else(|| missing("Expression.kind"))? {
        Kind::Literal(s) => Expression::Literal(scalar(s)?),
        Kind::Column(c) => Expression::Column(ColumnName::new(c.path)),
        Kind::Predicate(p) => Expression::Predicate(Box::new(predicate(*p)?)),
        Kind::StructExpr(s) => Expression::Struct(
            expr_refs(s.exprs)?,
            s.nullability_predicate
                .map(|n| Ok::<_, KernelError>(Arc::new(expression(*n)?)))
                .transpose()?,
        ),
        Kind::Transform(t) => Expression::StructPatch(ExpressionStructPatch {
            input_path: t.input_path.map(|c| ColumnName::new(c.path)),
            field_patches: t
                .field_transforms
                .into_iter()
                .map(|(name, ft)| {
                    Ok((
                        name,
                        ExpressionFieldPatch {
                            keep_input: !ft.is_replace,
                            insertions: expr_refs(ft.exprs)?,
                            optional: ft.optional,
                        },
                    ))
                })
                .collect::<KernelResult<_>>()?,
            prepended_fields: expr_refs(t.prepended_fields)?,
            appended_fields: expr_refs(t.appended_fields)?,
        }),
        Kind::Unary(u) => Expression::Unary(UnaryExpression {
            op: match pe::UnaryExpressionOp::try_from(u.op) {
                Ok(pe::UnaryExpressionOp::ToJson) => UnaryExpressionOp::ToJson,
                _ => return Err(KernelError::generic("decode: bad UnaryExpressionOp")),
            },
            expr: boxed(u.expr, "UnaryExpression.expr")?,
        }),
        Kind::Binary(b) => Expression::Binary(BinaryExpression {
            op: match pe::BinaryExpressionOp::try_from(b.op) {
                Ok(pe::BinaryExpressionOp::Plus) => BinaryExpressionOp::Plus,
                Ok(pe::BinaryExpressionOp::Minus) => BinaryExpressionOp::Minus,
                Ok(pe::BinaryExpressionOp::Multiply) => BinaryExpressionOp::Multiply,
                Ok(pe::BinaryExpressionOp::Divide) => BinaryExpressionOp::Divide,
                _ => return Err(KernelError::generic("decode: bad BinaryExpressionOp")),
            },
            left: boxed(b.left, "BinaryExpression.left")?,
            right: boxed(b.right, "BinaryExpression.right")?,
        }),
        Kind::Variadic(v) => Expression::Variadic(VariadicExpression {
            op: match pe::VariadicExpressionOp::try_from(v.op) {
                Ok(pe::VariadicExpressionOp::Coalesce) => VariadicExpressionOp::Coalesce,
                Ok(pe::VariadicExpressionOp::Array) => VariadicExpressionOp::Array,
                _ => return Err(KernelError::generic("decode: bad VariadicExpressionOp")),
            },
            exprs: v
                .exprs
                .into_iter()
                .map(expression)
                .collect::<KernelResult<_>>()?,
        }),
        Kind::CaseWhen(c) => Expression::CaseWhen(CaseWhenExpression {
            branches: c
                .branches
                .into_iter()
                .map(|b| {
                    Ok((
                        predicate(b.condition.ok_or_else(|| missing("CaseWhen.condition"))?)?,
                        expression(b.result.ok_or_else(|| missing("CaseWhen.result"))?)?,
                    ))
                })
                .collect::<KernelResult<_>>()?,
            else_expr: boxed(c.else_expr, "CaseWhen.else_expr")?,
        }),
        Kind::ParseJson(p) => Expression::ParseJson(ParseJsonExpression {
            json_expr: boxed(p.json_expr, "ParseJson.json_expr")?,
            output_schema: schema(p.output_schema)?,
        }),
        Kind::MapToStruct(m) => Expression::MapToStruct(MapToStructExpression {
            map_expr: boxed(m.map_expr, "MapToStruct.map_expr")?,
            options: match m.options.and_then(|o| o.timestamp_timezone) {
                Some(tz) => MapToStructOptions::default().with_timestamp_timezone(tz),
                None => MapToStructOptions::default(),
            },
        }),
        Kind::Opaque(o) => {
            return Err(KernelError::unsupported(format!(
                "decode: opaque expression '{}' cannot be reconstructed",
                o.name
            )));
        }
        Kind::Unknown(name) => {
            return Err(KernelError::unsupported(format!(
                "decode: unknown expression '{name}' cannot be reconstructed"
            )));
        }
    })
}

fn predicate(p: pe::Predicate) -> KernelResult<Predicate> {
    use pe::predicate::Kind;
    Ok(match p.kind.ok_or_else(|| missing("Predicate.kind"))? {
        Kind::BooleanExpression(e) => Predicate::BooleanExpression(expression(*e)?),
        Kind::Not(inner) => Predicate::Not(Box::new(predicate(*inner)?)),
        Kind::Unary(u) => Predicate::Unary(UnaryPredicate {
            op: match pe::UnaryPredicateOp::try_from(u.op) {
                Ok(pe::UnaryPredicateOp::IsNull) => UnaryPredicateOp::IsNull,
                _ => return Err(KernelError::generic("decode: bad UnaryPredicateOp")),
            },
            expr: boxed(u.expr, "UnaryPredicate.expr")?,
        }),
        Kind::Binary(b) => Predicate::Binary(BinaryPredicate {
            op: match pe::BinaryPredicateOp::try_from(b.op) {
                Ok(pe::BinaryPredicateOp::LessThan) => BinaryPredicateOp::LessThan,
                Ok(pe::BinaryPredicateOp::GreaterThan) => BinaryPredicateOp::GreaterThan,
                Ok(pe::BinaryPredicateOp::Equal) => BinaryPredicateOp::Equal,
                Ok(pe::BinaryPredicateOp::Distinct) => BinaryPredicateOp::Distinct,
                Ok(pe::BinaryPredicateOp::In) => BinaryPredicateOp::In,
                _ => return Err(KernelError::generic("decode: bad BinaryPredicateOp")),
            },
            left: boxed(b.left, "BinaryPredicate.left")?,
            right: boxed(b.right, "BinaryPredicate.right")?,
        }),
        Kind::Junction(j) => Predicate::Junction(JunctionPredicate {
            op: match pe::JunctionPredicateOp::try_from(j.op) {
                Ok(pe::JunctionPredicateOp::And) => JunctionPredicateOp::And,
                Ok(pe::JunctionPredicateOp::Or) => JunctionPredicateOp::Or,
                _ => return Err(KernelError::generic("decode: bad JunctionPredicateOp")),
            },
            preds: j
                .preds
                .into_iter()
                .map(predicate)
                .collect::<KernelResult<_>>()?,
        }),
        Kind::Opaque(o) => {
            return Err(KernelError::unsupported(format!(
                "decode: opaque predicate '{}' cannot be reconstructed",
                o.name
            )));
        }
        Kind::Unknown(name) => {
            return Err(KernelError::unsupported(format!(
                "decode: unknown predicate '{name}' cannot be reconstructed"
            )));
        }
    })
}

fn data_type(d: ps::DataType) -> KernelResult<DataType> {
    DataType::try_from(d)
}

fn scalar(s: pe::Scalar) -> KernelResult<Scalar> {
    use pe::scalar::Value;
    Ok(match s.value.ok_or_else(|| missing("Scalar.value"))? {
        Value::Integer(v) => Scalar::Integer(v),
        Value::Long(v) => Scalar::Long(v),
        Value::Short(v) => Scalar::Short(
            i16::try_from(v).map_err(|_| KernelError::generic("decode: short out of range"))?,
        ),
        Value::Byte(v) => Scalar::Byte(
            i8::try_from(v).map_err(|_| KernelError::generic("decode: byte out of range"))?,
        ),
        Value::Float(v) => Scalar::Float(v),
        Value::Double(v) => Scalar::Double(v),
        Value::String(v) => Scalar::String(v),
        Value::Boolean(v) => Scalar::Boolean(v),
        Value::Timestamp(v) => Scalar::Timestamp(v),
        Value::TimestampNtz(v) => Scalar::TimestampNtz(v),
        Value::Date(v) => Scalar::Date(v),
        Value::Binary(v) => Scalar::Binary(v),
        Value::IntervalYearMonth(m) => {
            Scalar::interval_year_month(m, IntervalYearToMonthType::IntervalYearToMonth)?
        }
        Value::IntervalDayTime(v) => Scalar::IntervalDayTime(v),
        Value::Decimal(d) => {
            let bits: [u8; 16] = d
                .bits
                .as_slice()
                .try_into()
                .map_err(|_| KernelError::generic("decode: decimal bits must be 16 bytes"))?;
            let ty = DecimalType::try_from(d.decimal_type.ok_or_else(|| missing("decimal_type"))?)?;
            Scalar::Decimal(DecimalData::try_new(i128::from_be_bytes(bits), ty)?)
        }
        Value::Null(d) => Scalar::Null(data_type(d)?),
        Value::Struct(s) => {
            let fields = s
                .fields
                .into_iter()
                .map(StructField::try_from)
                .collect::<KernelResult<Vec<_>>>()?;
            let values = s
                .values
                .into_iter()
                .map(scalar)
                .collect::<KernelResult<Vec<_>>>()?;
            Scalar::Struct(StructData::try_new(fields, values)?)
        }
        Value::Array(a) => {
            let ty = ArrayType::try_from(a.array_type.ok_or_else(|| missing("array_type"))?)?;
            let elements = a
                .elements
                .into_iter()
                .map(scalar)
                .collect::<KernelResult<Vec<_>>>()?;
            Scalar::Array(ArrayData::try_new(ty, elements)?)
        }
        Value::Map(m) => {
            let ty = MapType::try_from(m.map_type.ok_or_else(|| missing("map_type"))?)?;
            let pairs = m
                .pairs
                .into_iter()
                .map(|p| {
                    Ok((
                        scalar(p.key.ok_or_else(|| missing("MapEntry.key"))?)?,
                        scalar(p.value.ok_or_else(|| missing("MapEntry.value"))?)?,
                    ))
                })
                .collect::<KernelResult<Vec<_>>>()?;
            Scalar::Map(MapData::try_new(ty, pairs)?)
        }
    })
}

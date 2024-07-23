use crate::cast::as_map_array;
use crate::{project_schema, DataFusionError, DFSchemaRef};
use arrow::compute::{can_cast_types, cast};
use arrow_array::cast::{
    as_fixed_size_list_array, as_generic_list_array, as_struct_array,
};
use arrow_array::{
    new_null_array, Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray,
    MapArray, RecordBatch, RecordBatchOptions, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use log::{error, trace};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

/// Check whether an Arrow [DataType] is recursive in the sense that we need to
/// look inside and continue unpacking the data
/// This is used when creating a schema based on a deep projection
pub fn data_type_recurs(dt: &DataType) -> bool {
    match dt {
        // scalars
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Dictionary(_, _) => false,
        // containers
        DataType::RunEndEncoded(_, val) => data_type_recurs(val.data_type()),
        DataType::Union(_, _) => true,
        DataType::List(f) => data_type_recurs(f.data_type()),
        DataType::ListView(f) => data_type_recurs(f.data_type()),
        DataType::FixedSizeList(f, _) => data_type_recurs(f.data_type()),
        DataType::LargeList(f) => data_type_recurs(f.data_type()),
        DataType::LargeListView(f) => data_type_recurs(f.data_type()),
        // list of struct
        DataType::Map(f, _) => true,
        DataType::Struct(_) => true,
    }
}

/// Mutually recursive function with [rewrite_record_batch_field]
/// Given the source and destination fields, as well as the list of Arrow [Array]s
/// Returns the modified arrays as well as the modified fields.
pub fn rewrite_record_batch_fields(
    dst_fields: &Fields,
    src_fields: &Fields,
    arrays: Vec<ArrayRef>,
    num_rows: usize,
    fill_missing_source_fields: bool,
    error_on_missing_source_fields: bool,
) -> crate::Result<(Vec<ArrayRef>, Vec<FieldRef>)> {
    let mut out_arrays: Vec<ArrayRef> = vec![];
    let mut out_fields: Vec<FieldRef> = vec![];
    for i in 0..dst_fields.len() {
        let dst_field = dst_fields[i].clone();
        let dst_name = dst_field.name();

        let src_field_opt = src_fields
            .iter()
            .enumerate()
            .find(|(_idx, b)| b.name() == dst_name);

        // if the field exists in the source
        if src_field_opt.is_some() {
            let (src_idx, src_field) = src_field_opt.unwrap();
            let src_field = src_field.clone();
            let src_arr = arrays[src_idx].clone();
            let (tmp_array, tmp_field) = rewrite_record_batch_field(
                dst_field,
                src_field,
                src_arr,
                num_rows,
                fill_missing_source_fields,
                error_on_missing_source_fields,
            )?;
            out_arrays.push(tmp_array);
            out_fields.push(tmp_field);
        } else {
            if fill_missing_source_fields {
                let tmp_array = new_null_array(dst_field.data_type(), num_rows);
                out_arrays.push(tmp_array);
                out_fields.push(dst_field);
            } else if error_on_missing_source_fields {
                return Err(crate::DataFusionError::Internal(format!(
                    "field {dst_name} not found in source"
                )));
            }
        }
    }
    Ok((out_arrays, out_fields))
}

/// Mutually recursive function with [rewrite_record_batch_fields]
/// Rewrites a single field, returns a single modified field and array
/// If the field can be trivially casted (does not recur, simple data types) - falls back to Arrow functionality
/// If we have a container like field or struct, the function recurs for the data type inside
pub fn rewrite_record_batch_field(
    dst_field: FieldRef,
    src_field: FieldRef,
    src_array: ArrayRef,
    num_rows: usize,
    fill_missing_source_fields: bool,
    error_on_missing_source_fields: bool,
) -> crate::Result<(ArrayRef, FieldRef)> {
    let arrow_cast_available =
        can_cast_types(src_field.data_type(), dst_field.data_type());
    if arrow_cast_available {
        let casted_array = cast(src_array.as_ref(), dst_field.data_type())?;
        return Ok((casted_array, dst_field.clone()));
    }
    match (src_field.data_type(), dst_field.data_type()) {
        (DataType::List(src_inner), DataType::List(dst_inner)) => {
            if data_type_recurs(src_field.data_type()) {
                let src_array_clone = src_array.clone();

                let src_inner_list_array =
                    as_generic_list_array::<i32>(src_array_clone.as_ref()).clone();
                let src_offset_buffer = src_inner_list_array.offsets().clone();
                let src_nulls = match src_inner_list_array.nulls() {
                    None => None,
                    Some(x) => Some(x.clone()),
                };
                let (values, field) = rewrite_record_batch_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    src_inner_list_array.values().clone(),
                    num_rows,
                    fill_missing_source_fields,
                    error_on_missing_source_fields,
                )?;
                let nlarr = ListArray::try_new(
                    field.clone(),
                    src_offset_buffer,
                    values,
                    src_nulls,
                );
                let list_field = Arc::new(Field::new(
                    dst_field.name().clone(),
                    DataType::List(field.clone()),
                    dst_field.is_nullable(),
                ));

                Ok((Arc::new(nlarr.unwrap()), list_field))
            } else {
                let casted_array =
                    cast(src_array.as_ref(), dst_field.data_type())?;
                return Ok((casted_array, dst_field.clone()));
            }
        }
        (
            DataType::FixedSizeList(src_inner, src_sz),
            DataType::FixedSizeList(dst_inner, dst_sz),
        ) => {
            if src_sz != dst_sz {
                // Let Arrow do its thing, it's going to error
                let casted_array =
                    cast(src_array.as_ref(), dst_field.data_type())?;
                return Ok((casted_array, dst_field.clone()));
            }
            if data_type_recurs(src_field.data_type()) {
                let tmp = src_array.clone();
                let src_inner_list_array =
                    as_fixed_size_list_array(tmp.as_ref()).clone();
                let src_nulls = match src_inner_list_array.nulls() {
                    None => None,
                    Some(x) => Some(x.clone()),
                };
                let (values, field) = rewrite_record_batch_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    src_inner_list_array.values().clone(),
                    num_rows,
                    fill_missing_source_fields,
                    error_on_missing_source_fields,
                )?;

                let nlarr = FixedSizeListArray::try_new(
                    field.clone(),
                    *dst_sz,
                    values,
                    src_nulls,
                );
                let list_field = Arc::new(Field::new(
                    dst_field.name().clone(),
                    DataType::FixedSizeList(field.clone(), *dst_sz),
                    dst_field.is_nullable(),
                ));

                Ok((Arc::new(nlarr.unwrap()), list_field))
            } else {
                let casted_array =
                    cast(src_array.as_ref(), dst_field.data_type())?;
                return Ok((casted_array, dst_field.clone()));
            }
        }
        (DataType::LargeList(src_inner), DataType::LargeList(dst_inner)) => {
            if data_type_recurs(src_field.data_type()) {
                let tmp = src_array.clone();
                let src_inner_list_array =
                    as_generic_list_array::<i64>(tmp.as_ref()).clone();
                let src_offset_buffer = src_inner_list_array.offsets().clone();
                let src_nulls = match src_inner_list_array.nulls() {
                    None => None,
                    Some(x) => Some(x.clone()),
                };
                let (values, field) = rewrite_record_batch_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    src_inner_list_array.values().clone(),
                    num_rows,
                    fill_missing_source_fields,
                    error_on_missing_source_fields,
                )?;

                let nlarr = LargeListArray::try_new(
                    field.clone(),
                    src_offset_buffer,
                    values,
                    src_nulls,
                );
                let list_field = Arc::new(Field::new(
                    dst_field.name().clone(),
                    DataType::LargeList(field.clone()),
                    dst_field.is_nullable(),
                ));

                Ok((Arc::new(nlarr.unwrap()), list_field))
            } else {
                let casted_array =
                    cast(src_array.as_ref(), dst_field.data_type())?;
                return Ok((casted_array, dst_field.clone()));
            }
        }

        (DataType::Map(src_inner, _), DataType::Map(dst_inner, dst_ordered)) => {
            match (src_inner.data_type(), dst_inner.data_type()) {
                (DataType::Struct(src_inner_f), DataType::Struct(dst_inner_f)) => {
                    let src_map = as_map_array(src_array.as_ref())?;
                    let src_nulls = match src_map.nulls() {
                        None => None,
                        Some(x) => Some(x.clone()),
                    };
                    let src_offset_buffer = src_map.offsets().clone();

                    let (tmp_values_array, tmp_values_field) = rewrite_record_batch_field(
                        dst_inner_f[1].clone(),
                        src_inner_f[1].clone(),
                        src_map.values().clone(),
                        num_rows,
                        fill_missing_source_fields,
                        error_on_missing_source_fields,
                    )?;
                    // re-build map from keys and values after recursing only on the values
                    let entry_struct = StructArray::from(vec![
                        (dst_inner_f[0].clone(), src_map.keys().clone()),
                        (tmp_values_field, tmp_values_array),
                    ]);

                    let struct_field = Arc::new(Field::new(
                        dst_inner.name().clone(),
                        entry_struct.data_type().clone(),
                        false,
                    ));

                    let out_map = MapArray::try_new(
                        struct_field.clone(),
                        src_offset_buffer,
                        entry_struct,
                        src_nulls,
                        *dst_ordered,
                    )?;
                    let map_field = Arc::new(Field::new(
                        dst_field.name().clone(),
                        DataType::Map(struct_field.clone(), *dst_ordered),
                        dst_field.is_nullable(),
                    ));

                    Ok((Arc::new(out_map), map_field))
                }
                _ => unreachable!(), // unreachable
            }
        }

        (DataType::Struct(src_inner), DataType::Struct(dst_inner)) => {
            let src_struct_array = as_struct_array(src_array.as_ref());
            let src_nulls = match src_struct_array.nulls() {
                None => None,
                Some(x) => Some(x.clone()),
            };
            let src_columns = src_struct_array
                .columns()
                .iter()
                .map(|a| a.clone())
                .collect::<Vec<_>>();
            let (dst_columns, dst_fields) = rewrite_record_batch_fields(
                dst_inner,
                src_inner,
                src_columns,
                num_rows,
                fill_missing_source_fields,
                error_on_missing_source_fields,
            )?;
            let struct_array =
                StructArray::try_new(dst_inner.clone(), dst_columns, src_nulls)
                    .map_err(|ae| DataFusionError::from(ae))?;
            let struct_field = Field::new_struct(
                dst_field.name(),
                dst_fields,
                dst_field.is_nullable(),
            );
            Ok((Arc::new(struct_array), Arc::new(struct_field)))
        }
        _ => {
            Err(DataFusionError::Internal(format!(
                "Could not remap field src type={}, dst type={}",
                src_field.data_type(), dst_field.data_type()
            )))
        }
    }
}

/// Rewrites a record batch with a source schema to match a destination schema
pub fn try_rewrite_record_batch(
    src: SchemaRef,
    src_record_batch: RecordBatch,
    dst: SchemaRef,
    fill_missing_source_fields: bool,
    error_on_missing_source_fields: bool,
) -> crate::Result<RecordBatch> {

    let num_rows = src_record_batch.num_rows();
    let (final_columns, final_fields) = rewrite_record_batch_fields(
        dst.fields(),
        src.fields(),
        src_record_batch.columns().into(),
        num_rows,
        fill_missing_source_fields,
        error_on_missing_source_fields,
    )?;

    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    let schema = Arc::new(Schema::new(final_fields));
    let record_batch =
        RecordBatch::try_new_with_options(schema, final_columns, &options)?;
    Ok(record_batch)
}

/// Rewrites a record batch with a source schema to match a destination schema
/// The destination schema is further filtered using the mappings
pub fn try_rewrite_record_batch_with_mappings(
    src: SchemaRef,
    src_record_batch: RecordBatch,
    dst: SchemaRef,
    mappings: Vec<Option<usize>>,
) -> crate::Result<RecordBatch> {
    let src_record_batch_cols = src_record_batch.columns().to_vec();
    let num_rows = src_record_batch.num_rows();
    let field_vecs = dst.fields()
        .iter()
        .zip(mappings)
        .map(|(field, src_idx)| match src_idx {
            Some(batch_idx) => {
                let arr = src_record_batch_cols[batch_idx].clone();
                let src_field = Arc::new(src.field(batch_idx).clone());
                rewrite_record_batch_field(field.clone(), src_field, arr, num_rows, true, false)
            },
            None => Ok((new_null_array(field.data_type(), num_rows), field.clone())),
        })
        .collect::<crate::Result<Vec<(_, _)>, _>>()?;

    let mut final_columns: Vec<ArrayRef> = vec![];
    let mut final_fields: Vec<FieldRef> = vec![];
    for (a, f) in field_vecs {
        final_columns.push(a);
        final_fields.push(f);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    let schema = Arc::new(Schema::new(final_fields));
    let record_batch =
        RecordBatch::try_new_with_options(schema, final_columns, &options)?;
    Ok(record_batch)
}

/// Mutually recursive with [can_rewrite_field]
/// checks whether we can rewrite a source [Fields] object to a destination one
/// the missing fields in the source behavior can be changed with the [`fill_missing_source_field`]
/// parameter.
pub fn can_rewrite_fields(
    dst_fields: &Fields,
    src_fields: &Fields,
    fill_missing_source_fields: bool,
) -> bool {
    let mut out = true;
    for i in 0..dst_fields.len() {
        let dst_field = dst_fields[i].clone();
        let dst_name = dst_field.name();

        let src_field_opt = src_fields
            .iter()
            .enumerate()
            .find(|(_idx, b)| b.name() == dst_name);

        // if the field exists in the source
        if src_field_opt.is_some() {
            let (_src_idx, src_field) = src_field_opt.unwrap();
            let src_field = src_field.clone();
            let can_cast =
                can_rewrite_field(dst_field, src_field, fill_missing_source_fields);
            out = out && can_cast;
        } else {
            out = out && fill_missing_source_fields;
        }
    }
    out
}

/// Mutually recursive with [can_rewrite_fielda]
/// checks whether we can rewrite a source [FieldRef] object to a destination one
/// the missing fields in the source behavior can be changed with the [`fill_missing_source_field`]
/// parameter.
pub fn can_rewrite_field(
    dst_field: FieldRef,
    src_field: FieldRef,
    fill_missing_source_fields: bool,
) -> bool {
    let can_cast_by_arrow = !data_type_recurs(dst_field.data_type())
        && !data_type_recurs(src_field.data_type());
    if can_cast_by_arrow {
        return can_cast_types(src_field.data_type(), dst_field.data_type());
    }
    match (src_field.data_type(), dst_field.data_type()) {
        (DataType::List(src_inner), DataType::List(dst_inner))
        | (DataType::List(src_inner), DataType::LargeList(dst_inner))
        | (DataType::LargeList(src_inner), DataType::LargeList(dst_inner)) => {
            if data_type_recurs(src_inner.data_type())
                && data_type_recurs(dst_inner.data_type())
            {
                return can_rewrite_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    fill_missing_source_fields,
                );
            } else {
                return can_cast_types(src_inner.data_type(), dst_inner.data_type());
            }
        }
        (
            DataType::FixedSizeList(src_inner, src_sz),
            DataType::FixedSizeList(dst_inner, dst_sz),
        ) => {
            if src_sz != dst_sz {
                return false;
            }
            if data_type_recurs(src_inner.data_type())
                && data_type_recurs(dst_inner.data_type())
            {
                return can_rewrite_field(
                    dst_inner.clone(),
                    src_inner.clone(),
                    fill_missing_source_fields,
                );
            } else {
                return can_cast_types(src_inner.data_type(), dst_inner.data_type());
            }
        }
        (DataType::Map(src_inner, _), DataType::Map(dst_inner, _)) => {
            return match (src_inner.data_type(), dst_inner.data_type()) {
                (DataType::Struct(src_inner_f), DataType::Struct(dst_inner_f)) => {
                    can_rewrite_field(
                        dst_inner_f[1].clone(),
                        src_inner_f[1].clone(),
                        fill_missing_source_fields,
                    )
                }
                _ => false
            }
        }
        (DataType::Struct(src_inner), DataType::Struct(dst_inner)) => {
            return can_rewrite_fields(dst_inner, src_inner, fill_missing_source_fields);
        }
        (_src, _dest) => {
            error!(
                target: "deep",
                "  can_rewrite_field: Unhandled src dest field: src {}={:?}, dst {}={:?}",
                src_field.name(),
                src_field.data_type(),
                dst_field.name(),
                dst_field.data_type()
            );
            false
        },
    }
}

/// Deep projections are represented using a HashMap<usize, Vec<String>>
/// for backwards compatibility (current projections are represented using a [`Vec<usize>`]
/// Currently, deep projections are represented (even if there's some duplicated information as a
/// [`HashMap<usize, Vec<String>>`]
/// the key is the source field id of the top-level field
/// the value is a list of "paths" inside the top-level field
/// Examples:
///   Scalar fields - no representations of paths inside the field possible
///   List<Scalar> fields - same thing
///   List<Struct<id, name, address>> - possible paths may be "*.id", "*.name", "*.address"
///   List<Map<String, Map<String, Struct<id, name, address>>>
///     - possible paths may be "*.*", "*.*.*.id", "*.*.*.name", "*.*.*.address"
pub fn has_deep_projection(possible: Option<&HashMap<usize, Vec<String>>>) -> bool {
    if possible.is_none() {
        return false;
    }
    let tmp = possible.unwrap();
    !(tmp.is_empty() || tmp.iter().all(|(k, v)| v.len() == 0))
}

/// Combines the current projection (numeric indices of top-level columns) with
/// the deep projection - "paths" inside a top-level column
pub fn splat_columns(
    src: SchemaRef,
    projection: &Vec<usize>,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for pi in projection.iter() {
        let f = src.field(*pi);
        match projection_deep.get(pi) {
            None => {
                out.push(f.name().to_owned());
            }
            Some(rests) => {
                if rests.len() > 0 {
                    for rest in rests.iter() {
                        match f.data_type() {
                            _ => out.push(format!("{}.{}", f.name(), rest)),
                        }
                    }
                } else {
                    out.push(f.name().to_owned());
                }
            }
        }
    }
    out
}

pub fn try_rewrite_schema_opt(
    src: SchemaRef,
    projection_opt: Option<&Vec<usize>>,
    projection_deep_opt: Option<&HashMap<usize, Vec<String>>>,
) -> crate::Result<SchemaRef> {
    match projection_opt {
        None => Ok(src),
        Some(projection) => match projection_deep_opt {
            None => project_schema(&src, projection_opt),
            Some(projection_deep) => Ok(rewrite_schema(src, projection, projection_deep)),
        },
    }
}

pub fn rewrite_field_projection(
    src: SchemaRef,
    projected_field_idx: usize,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> FieldRef {
    let original_field = Arc::new(src.field(projected_field_idx).clone());
    let single_field_schema = Arc::new(Schema::new(vec![original_field]));
    // rewrite projection, deep projection to use 0
    let projected_vec = vec![0];
    let mut projected_deep_vec = HashMap::new();
    let empty_vec: Vec<String> = vec![];
    projected_deep_vec.insert(
        0 as usize,
        projection_deep
            .get(&projected_field_idx)
            .unwrap_or(&empty_vec)
            .clone(),
    );

    let rewritten_single_field_schema =
        rewrite_schema(single_field_schema, &projected_vec, &projected_deep_vec);
    Arc::new(rewritten_single_field_schema.field(0).clone())
}

fn make_path(parent: &String, name: &str) -> String {
    if parent == "" {
        return name.to_owned();
    } else {
        return format!("{}.{}", parent, name);
    }
}

fn path_prefix_exists(filters: &Vec<String>, path: &String) -> bool {
    filters.iter().any(|f| {
        let tmp = f.find(path);
        tmp.is_some() && tmp.unwrap() == 0
    })
}

fn path_included(filters: &Vec<String>, path: &String) -> bool {
    filters.iter().any(|f| {
        let tmp = path.find(f);
        tmp.is_some() && tmp.unwrap() == 0
    })
}

pub fn fix_possible_field_accesses(schema: &DFSchemaRef, field_idx: usize, rest: &Vec<String>) -> crate::Result<Vec<String>> {
    let mut field = Arc::new(schema.field(field_idx).clone());
    let mut rest_idx = 0 as usize;
    let mut out = rest.clone();
    while rest_idx < out.len() {
        let (fix_non_star_access, should_continue, new_field) = match field.data_type() {
            DataType::Null  | DataType::Boolean  | DataType::Int8  | DataType::Int16  | DataType::Int32  |
            DataType::Int64  | DataType::UInt8  | DataType::UInt16  | DataType::UInt32  | DataType::UInt64  |
            DataType::Float16  | DataType::Float32  | DataType::Float64  | DataType::Timestamp(_, _)  |
            DataType::Date32  | DataType::Date64  | DataType::Time32(_)  | DataType::Time64(_)  | DataType::Duration(_)  |
            DataType::Interval(_)  | DataType::Binary  | DataType::FixedSizeBinary(_)  |
            DataType::LargeBinary  | DataType::BinaryView  | DataType::Utf8  | DataType::LargeUtf8  | DataType::Utf8View |
            DataType::Dictionary(_, _) | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) |
            DataType::RunEndEncoded(_, _)=> {
                (false, false, None)
            }
            DataType::Union(_, _) => {
                // FIXME @HStack
                // don't know what to do here
                (false, false, None)
            }
            DataType::List(inner) | DataType::ListView(inner) |
            DataType::FixedSizeList(inner, _) | DataType::LargeList(inner) |
            DataType::LargeListView(inner) => {
                let new_field = inner.clone();
                (true, true, Some(new_field))
            }
            DataType::Struct(inner_struct) => {
                let mut new_field: Option<FieldRef> = None;
                for f in inner_struct.iter() {
                    if f.name() == &out[rest_idx] {
                        new_field = Some(f.clone());
                    }
                }
                (false, true, new_field)
            }
            DataType::Map(inner_map, _) => {
                let mut new_field: Option<FieldRef> = None;
                match inner_map.data_type() {
                    DataType::Struct(inner_map_struct) => {
                        new_field = Some(inner_map_struct[1].clone());
                    }
                    _ => {
                        return Err(DataFusionError::Internal(String::from("Invalid inner map type")));
                    }
                }
                (true, true, new_field)
            }
        };
        if fix_non_star_access && rest[rest_idx] != "*" {
            out[rest_idx] = "*".to_string();
        }
        if !should_continue {
            break;
        }
        field = new_field.unwrap();
        rest_idx += 1;
    }
    Ok(out)
}

pub fn rewrite_schema(
    src: SchemaRef,
    projection: &Vec<usize>,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> SchemaRef {
    fn rewrite_schema_fields(
        parent: String,
        src_fields: &Fields,
        filters: &Vec<String>,
    ) -> Vec<FieldRef> {
        let mut out_fields: Vec<FieldRef> = vec![];
        for i in 0..src_fields.len() {
            let src_field = src_fields[i].clone();
            let src_field_path = make_path(&parent, src_field.name());

            let field_path_included = path_included(filters, &src_field_path); //filters.contains(&src_field_path);
            if field_path_included {
                out_fields.push(src_field.clone());
            } else {
                if data_type_recurs(src_field.data_type())
                    && path_prefix_exists(filters, &src_field_path)
                {
                    match rewrite_schema_field(parent.clone(), src_field, filters) {
                        None => {}
                        Some(f) => out_fields.push(f),
                    }
                }
            }
        }
        out_fields
    }

    fn rewrite_schema_field(
        parent: String,
        src_field: FieldRef,
        filters: &Vec<String>,
    ) -> Option<FieldRef> {
        let src_field_name = src_field.name();
        // FIXME: @HStack
        //  if we already navigated to this field and the accessor is "*"
        //  that means we don't care about the field name
        //  RETEST THIS for lists
        let src_field_path = if parent.len() > 0 && parent.chars().last().unwrap() == '*' as char {
            parent.clone()
        } else {
            make_path(&parent, src_field_name)
        };
        trace!(target:"deep", "rewrite field: {} = {} ({:?})", src_field_name, src_field_path, filters);

        let field_path_included = path_included(filters, &src_field_path); //filters.contains(&src_field_path);
        if field_path_included {
            trace!(target:"deep", "  return {} directly ", src_field_path);
            return Some(src_field.clone());
        } else {
            if data_type_recurs(src_field.data_type())
                && path_prefix_exists(filters, &src_field_path)
            {
                let out = match src_field.data_type() {
                    DataType::List(src_inner) => {
                        rewrite_schema_field(
                            make_path(&src_field_path, "*"),
                            src_inner.clone(),
                            filters,
                        )
                        .map(|inner| {
                            trace!(target:"deep", "return new list {} = {:#?}", src_field_name.clone(), inner.clone());
                            Arc::new(Field::new_list(
                                src_field.name(),
                                inner,
                                src_field.is_nullable(),
                            ))
                        })
                    }
                    DataType::FixedSizeList(src_inner, src_sz) => rewrite_schema_field(
                        make_path(&src_field_path, "*"),
                        src_inner.clone(),
                        filters,
                    )
                    .map(|inner| {
                        Arc::new(Field::new_fixed_size_list(
                            src_field.name(),
                            inner,
                            *src_sz,
                            src_field.is_nullable(),
                        ))
                    }),
                    DataType::LargeList(src_inner) => rewrite_schema_field(
                        make_path(&src_field_path, "*"),
                        src_inner.clone(),
                        filters,
                    )
                    .map(|inner| {
                        Arc::new(Field::new_large_list(
                            src_field.name(),
                            inner,
                            src_field.is_nullable(),
                        ))
                    }),

                    DataType::Map(map_entry, map_sorted) => {
                        if let DataType::Struct(map_entry_fields) = map_entry.data_type()
                        {
                            let map_key_field = map_entry_fields.get(0).unwrap();
                            let map_value_field = map_entry_fields.get(1).unwrap();
                            rewrite_schema_field(
                                make_path(&src_field_path, "*"),
                                map_value_field.clone(),
                                filters,
                            )
                            .map(|inner| {
                                Arc::new(Field::new_map(
                                    src_field_name,
                                    map_entry.name().clone(),
                                    map_key_field.clone(),
                                    inner,
                                    *map_sorted,
                                    src_field.is_nullable(),
                                ))
                            })
                        } else {
                            panic!("Invalid internal field map: expected struct, but got {}", map_entry.data_type());
                        }
                    }

                    DataType::Struct(src_inner) => {
                        let dst_fields =
                            rewrite_schema_fields(src_field_path.clone(), src_inner, filters);
                        trace!(target:"deep", "for struct: {} {} = {:#?}", src_field_name, src_field_path.clone(), dst_fields);
                        if dst_fields.len() > 0 {
                            Some(Arc::new(Field::new_struct(
                                src_field.name(),
                                dst_fields,
                                src_field.is_nullable(),
                            )))
                        } else {
                            None
                        }
                    }
                    x => {
                        panic!("Unhandled data type: {:#?}", x);
                    }
                };
                out
            } else {
                None
            }
        }
    }

    let actual_projection = if projection.len() == 0 {
        (0..src.fields().len()).collect()
    } else {
        projection.clone()
    };
    let splatted = splat_columns(src.clone(), &actual_projection, &projection_deep);

    // trace!(target:"deep", "rewrite_schema source: {:#?}", src);
    trace!(target:"deep", "rewrite_schema splatted: {:?} {:?} = {:?}", &actual_projection, &projection_deep, splatted);
    let mut dst_fields: Vec<FieldRef> = vec![];
    for pi in actual_projection.iter() {
        let src_field = src.field(*pi);
        trace!(target:"deep", "rewrite_schema at field {}", src_field.name());
        let foutopt =
            rewrite_schema_field("".to_string(), Arc::new(src_field.clone()), &splatted);
        match foutopt {
            None => {}
            Some(fout) => {
                dst_fields.push(fout.clone());
            }
        }
    }

    // let dst_fields = rewrite_fields("".to_string(), src.clone().fields(), &splatted);
    // trace!(target:"deep", "rewrite_schema dst: {:#?}", dst_fields);

    let output = if dst_fields.len() > 0 {
        Arc::new(Schema::new_with_metadata(dst_fields, src.metadata.clone()))
    } else {
        src.clone()
    };

    return output;
}

pub fn debug_to_file(name: &str, contents: &str) {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(name)
        .unwrap();
    file.write_all(contents.as_bytes()).unwrap();
}

#[cfg(test)]
mod tests {
    use crate::deep::can_rewrite_fields;
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_cast() -> crate::error::Result<()> {
        // source, destination, is_fill_dependent
        let cases = vec![
            (
                Arc::new(Schema::new(vec![Field::new("i1", DataType::Int32, true)])),
                Arc::new(Schema::new(vec![Field::new("i1", DataType::Int8, true)])),
                false,
                true,
            ),
            (
                Arc::new(Schema::new(vec![Field::new("i1", DataType::Int32, true)])),
                Arc::new(Schema::new(vec![Field::new(
                    "i1",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "s1",
                        DataType::Utf8,
                        true,
                    )])),
                    true,
                )])),
                false,
                false,
            ),
            (
                Arc::new(Schema::new(vec![Field::new(
                    "l1",
                    DataType::List(Arc::new(Field::new(
                        "s1",
                        DataType::Struct(Fields::from(vec![
                            Field::new("s1extra1", DataType::Utf8, true),
                            Field::new("s1extra2", DataType::Utf8, true),
                            Field::new("s1i2", DataType::Int32, true),
                            Field::new("s1s1", DataType::Utf8, true),
                            Field::new(
                                "s1m1",
                                DataType::Map(
                                    Arc::new(Field::new(
                                        "entries",
                                        DataType::Struct(Fields::from(vec![
                                            Field::new("key", DataType::Utf8, false),
                                            Field::new("value", DataType::Utf8, false),
                                        ])),
                                        true,
                                    )),
                                    false,
                                ),
                                true,
                            ),
                            Field::new(
                                "s1l1",
                                DataType::List(Arc::new(Field::new(
                                    "s1l1i1",
                                    DataType::Int32,
                                    true,
                                ))),
                                true,
                            ),
                        ])),
                        true,
                    ))),
                    true,
                )])),
                Arc::new(Schema::new(vec![Field::new(
                    "l1",
                    DataType::List(Arc::new(Field::new(
                        "s1",
                        DataType::Struct(Fields::from(vec![
                            Field::new("s1s1", DataType::Utf8, true),
                            Field::new("s1i2", DataType::Int32, true),
                            Field::new(
                                "s1m1",
                                DataType::Map(
                                    Arc::new(Field::new(
                                        "entries",
                                        DataType::Struct(Fields::from(vec![
                                            Field::new("key", DataType::Utf8, false),
                                            Field::new("value", DataType::Utf8, false),
                                        ])),
                                        true,
                                    )),
                                    false,
                                ),
                                true,
                            ),
                            Field::new(
                                "s1l1",
                                DataType::List(Arc::new(Field::new(
                                    "s1l1i1",
                                    DataType::Date32,
                                    true,
                                ))),
                                true,
                            ),
                            // extra field
                            Field::new("s1ts1", DataType::Time32(TimeUnit::Second), true),
                        ])),
                        true,
                    ))),
                    true,
                )])),
                true,
                true,
            ),
        ];
        for (from, to, can_fill, res) in cases.iter() {
            assert_eq!(
                can_rewrite_fields(from.fields(), to.fields(), *can_fill),
                *res,
                "Wrong result"
            );
        }
        Ok(())
    }
}

use super::*;

#[test]
fn test_dictionary_type_coercion() {
    use DataType::*;

    let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(Int32)
    );
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
        Some(Int32)
    );

    // Since we can coerce values of Int16 to Utf8 can support this
    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(Utf8)
    );

    // Since we can coerce values of Utf8 to Binary can support this
    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(Binary)
    );

    let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let rhs_type = Utf8;
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
        Some(Utf8)
    );
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(lhs_type.clone())
    );

    let lhs_type = Utf8;
    let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
        Some(Utf8)
    );
    assert_eq!(
        dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
        Some(rhs_type.clone())
    );
}

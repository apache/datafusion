pub fn is_system_variables(variable_names: &[String]) -> bool {
    ! variable_names.is_empty() && variable_names[0].get(0..2) == Some("@@")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_system_variables() {
        assert!(!is_system_variables(&["N".into(), "\"\"".into()]));
        assert!(is_system_variables(&["@@D".into(), "F".into(), "J".into()]));
        assert!(!is_system_variables(&["J".into(), "@@F".into(), "K".into()]));
        assert!(!is_system_variables(&[]));
        assert!(is_system_variables(&["@@longvariablenamethatIdontknowhwyanyonewoulduse".into()]))
    }
}
#[macro_export]
macro_rules! doc_cfg {
    () => {
        #![doc(
            html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/docs/logos/standalone_logo/logo_original.svg",
            html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/docs/logos/standalone_logo/logo_original.svg"
        )]
        #![cfg_attr(docsrs, feature(doc_auto_cfg))]
    };
}

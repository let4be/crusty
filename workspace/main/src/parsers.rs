#[cfg(feature = "html5ever")]
pub(crate) mod html5ever_defs;
#[cfg(feature = "lol_html_parser")]
pub(crate) mod lolhtml_defs;

#[cfg(feature = "html5ever")]
pub(crate) mod html5ever;
#[cfg(feature = "lol_html_parser")]
pub(crate) mod lolhtml;

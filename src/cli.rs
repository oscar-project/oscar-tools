use structopt::StructOpt;
use std::path::PathBuf;
use crate::lang_codes::UpdateLangCodes as UpdateLangCodes;
// #[derive(Debug, StructOpt)]
// #[structopt(name = "oscar-tools", about = "A collection of tools for OSCAR corpus")]
// pub struct Opt {
//     #[structopt(parse(from_os_str))]
//     oscar_dir: PathBuf,
// }

#[derive(Debug, StructOpt)]
#[structopt(name = "oscar-tools", about = "A collection of tools for OSCAR corpus")]
pub enum OscarTools {
    #[structopt(about="update language codes from ISO 639-1 to ISO 639-3")]
    UpdateLangCodes {
        #[structopt(short, help="Dry run")]
        dry: bool,
    }
}
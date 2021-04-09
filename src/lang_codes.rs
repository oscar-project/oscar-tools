use structopt::StructOpt;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
pub struct UpdateLangCodes {
    #[structopt(short, help="Dry run")]
    dry: bool,

    #[structopt(parse(from_os_str))]
    oscar_path: PathBuf,
}
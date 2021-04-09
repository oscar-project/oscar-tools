mod cli;
mod lang_codes;

use cli::OscarTools;
use structopt::StructOpt;

fn main() {
    let opt = OscarTools::from_args();
    println!("{:?}", opt);
}

use crate::cli::Runnable;
use crate::error::Error;
use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
/// updates language codes in two ways.
/// - wrong codes (als -> gsw)
/// - obsolete codes (sh -> (sr|hr|bs)), (eml -> (egl|rgn))
/// - non BCP-47 codes (not yet implemented)
pub struct UpdateLangCodes {
    #[structopt(
        short,
        long,
        help = "Dry run: Does not change anything but gives the affected languages/files"
    )]
    dry: bool,

    #[structopt(parse(from_os_str))]
    oscar_path: PathBuf,
}

impl Runnable for UpdateLangCodes {
    fn run(&self) -> Result<(), Error> {
        // stores fixes (key is the wrong code, value is the right one)
        let mut fixes = HashMap::new();
        fixes.insert("als", "gsw");
        // TODO: currently dumb fixing it, but it may be important to decide between
        // the replacements
        fixes.insert("sh", "sr");
        fixes.insert("eml", "egl");

        debug!("language fixes {:#?}", fixes);

        for entry in self.oscar_path.read_dir()? {
            //get the lang id for each file
            let entry_path = entry?.path();
            let entry = entry_path.file_stem();

            match entry {
                Some(lang) => {
                    // let lang = lang.to_str().ok_or(Error::Custom(format!(
                    //     "language file name is not a unicode string: {:?}",
                    //     lang
                    // )))?;

                    let lang = lang.to_str().ok_or_else(|| {
                        Error::Custom(format!(
                            "language file name is not a unicode string: {:?}",
                            lang
                        ))
                    })?;

                    // apply fix if language is in the fix list
                    if let Some(fix) = fixes.get(&lang) {
                        // create a new path and change the filename
                        let mut new_path = PathBuf::from(&entry_path);
                        new_path.set_file_name(fix);
                        new_path.set_extension("txt");

                        // only apply change if no dry run flag
                        if self.dry {
                            info!("would move {:?} to {:?}", &entry_path, &new_path);
                        } else {
                            info!("moving {:?} to {:?}", &entry_path, &new_path);
                            std::fs::rename(entry_path, new_path)?;
                        }
                    }
                }
                None => error!("warn: {:?} has no file name", entry),
            }
        }

        Ok(())
    }
}

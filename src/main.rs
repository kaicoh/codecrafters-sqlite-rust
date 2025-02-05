use clap::Parser;
use codecrafters_sqlite::{db::DbFile, Cli, Error, Result};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let Cli { db_path, command } = Cli::parse();
    let mut db = DbFile::from_path(db_path)?;

    match command.as_str() {
        ".dbinfo" => {
            let page_size = db.file_header()?.page_size();
            let num_tables = db.num_tables()?;

            println!("database page size: {}", page_size);
            println!("number of tables: {}", num_tables);
        }
        _ => {
            return Err(Error::Other(anyhow::anyhow!("Unknown command: {command}")));
        }
    }

    Ok(())
}

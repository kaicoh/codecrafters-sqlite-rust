use clap::Parser;
use codecrafters_sqlite::{db::DbFile, err, Cli, Result};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let Cli { db_path, command } = Cli::parse();
    let db = DbFile::from_path(db_path)?;

    match command.as_str() {
        ".dbinfo" => {
            let page_size = db.file_header()?.page_size();
            let num_tables = db.num_tables()?;

            println!("database page size: {}", page_size);
            println!("number of tables: {}", num_tables);
        }
        ".tables" => {
            let tables = db.table_names()?.join(" ");
            println!("{tables}");
        }
        cmd if cmd.starts_with("select count(*) from ") => {
            let name = cmd.trim_start_matches("select count(*) from ");
            let table = db.table(name)?;
            println!("{}", table.rows()?.count());
        }
        _ => {
            return Err(err!("Unknown command: {command}"));
        }
    }

    Ok(())
}

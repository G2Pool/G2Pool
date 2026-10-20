fn main() {
    if let Err(error) = g2pool::cli::run() {
        eprintln!("g2pool: {error}");
        std::process::exit(1);
    }
}

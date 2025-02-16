use clap::Parser;

#[derive(Parser)]
pub struct ServiceArguments {
    #[arg(long)]
    pub dir: Option<String>,

    #[arg(long)]
    pub dbfilename: Option<String>,

    #[arg(long)]
    pub port: Option<usize>,

    #[arg(long)]
    pub replicaof: Option<String>,
}

use clap::{App, AppSettings, Arg, SubCommand};
use std::process::exit;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set key-value pair")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("A string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the value of given specific key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove the given key and associated value")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", Some(_)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", Some(_)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}

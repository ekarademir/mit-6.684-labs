extern crate termion;

use std::io::{self, Read, Write};

use termion::{clear, cursor};
use termion::raw::IntoRawMode;
use termion::input::TermRead;
use termion::event::Key;
use termion::event::Key::*;


struct MachineState {
    logs: String,
    buffers: String,
    identity: String,
    cluster: String
}


fn main() {
    let terminal_size = termion::terminal_size().ok();

    let stdin = io::stdin();
    let stdout = io::stdout();
    let stdout = stdout.lock();
    let stdin = stdin.lock();
    let mut keys = stdin.keys();
    let mut stdout = stdout.into_raw_mode().unwrap();

    write!(stdout, "{}", clear::All).unwrap();
    stdout.flush().unwrap();
    loop {
        let k = keys.next().unwrap().unwrap();
        match k {
            Esc => {
                return;
            },
            Char(x) => {
                write!(stdout,
                    "{}{}",
                    cursor::Goto(1, 1),
                    x
                ).unwrap();
                stdout.flush().unwrap();
            },
            _ => {}
        }
    }
}

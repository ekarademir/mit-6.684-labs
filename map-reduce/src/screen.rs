use std::io::{self, Read, Write};
use std::thread;
use std::time::Duration;

use termion::{clear, cursor};
use termion::raw::IntoRawMode;
use termion::async_stdin;
use termion::event;

const DOUBLE_LINE_VERTICAL: &str = "║";

pub struct Screen {

}

impl Screen {
    pub fn new() -> Screen {
        Screen{}
    }

    fn render(self: &Screen) -> String {
        format!("{}", DOUBLE_LINE_VERTICAL)
    }

    pub fn start(self: &Screen) {
        let terminal_size = termion::terminal_size().ok();
        let stdout = io::stdout();
        let mut stdout = stdout.lock().into_raw_mode().unwrap();

        // Hide the cursor at the start
        write!(stdout, "{}", cursor::Hide).unwrap();
        stdout.flush().unwrap();

        // Event loop to asynchroniously listen to keyboard inputs
        let mut stdin = async_stdin().bytes();
        loop {
            // Read the next byte
            let b = stdin.next();
            match b {
                Some(r) => { match r {
                    Ok(x) => {
                        // Parse the incoming byte
                        let interrupt = event::parse_event(x, &mut stdin);
                        // Try to match the incoming byte with a key
                        // Unfortunatelly, special keys won't work.
                        match interrupt {
                            Ok(e) => {
                                // Escape program
                                match e {event::Event::Key(event::Key::Char('q')) => {
                                    // Show cursur again and clean up
                                    write!(stdout, "{}{}", cursor::Show, clear::All).unwrap();
                                    stdout.flush().unwrap();
                                    return
                                },
                                _ => {}}
                            },
                            _ => {}
                        };
                    },
                    _ => {}
                }},
                _ => {}
            }

            write!(stdout, "{}", clear::All).unwrap();
            stdout.flush().unwrap();
            write!(stdout, "{}{}", cursor::Goto(1,1), self.render()).unwrap();
            stdout.flush().unwrap();
            thread::sleep(Duration::from_millis(500));
        }
    }
}

use std::fs;
use std::env;

use env_logger;
use log::{debug};

fn main () {
    env_logger::try_init().expect("Can't initialize logger");

    let mut path = env::current_dir().unwrap();
    path.push("./generate-case/data/oxford_5000.txt");
    let file_path = path.canonicalize().unwrap();
    debug!("Reading from {:?}", file_path);

    match fs::read_to_string(file_path) {
        Ok(contents) => {
            let words = parse_file(&contents);

        },
        Err(e) => {
            println!("Error reading the file {:?}", e);
        }
    }
}

#[derive(Debug)]
enum PartOfSpeech {
    Verb,
    Noun,
    Adjective,
    Adverb,
    Preposition,
    Conjuction,
    Pronoun,
    Number,
}

#[derive(Debug, PartialEq)]
enum LangLevel {
    A1,
    A2,
    B1,
    B2,
    C1,
    C2,
    Unknown,
}

struct AWord {
    word: String,
    level: LangLevel,
    pos: Vec<PartOfSpeech>,
}

fn parse_file(contents: &String) -> Vec<AWord> {
    let mut words = Vec::new();
    for line in contents.split("\n") {
        let (
            word,
            part_of_speech,
            level,
        ) = {
            let mut word: String = String::new();
            let mut part_of_speech: Vec<PartOfSpeech> = Vec::new();
            let mut level: LangLevel = LangLevel::Unknown;
            for part in line.trim().split(" ").collect::<Vec<&str>>().iter().rev() {
                match (*part).trim_end_matches(',') {
                    "A1" => {
                        level = LangLevel::A1
                    },
                    "A2" => {
                        level = LangLevel::A2
                    },
                    "B1" => {
                        level = LangLevel::B1
                    },
                    "B2" => {
                        level = LangLevel::B2
                    },
                    "C1" => {
                        level = LangLevel::C1
                    },
                    "C2" => {
                        level = LangLevel::C2
                    },
                    "v." => {
                        part_of_speech.push(PartOfSpeech::Verb)
                    },
                    "n." => {
                        part_of_speech.push(PartOfSpeech::Noun)
                    },
                    "adj." => {
                        part_of_speech.push(PartOfSpeech::Adjective)
                    },
                    "adv." => {
                        part_of_speech.push(PartOfSpeech::Adverb)
                    },
                    "prep." => {
                        part_of_speech.push(PartOfSpeech::Preposition)
                    },
                    "conj." => {
                        part_of_speech.push(PartOfSpeech::Conjuction)
                    },
                    "pron." => {
                        part_of_speech.push(PartOfSpeech::Pronoun)
                    },
                    "number" => {
                        part_of_speech.push(PartOfSpeech::Number);
                        part_of_speech.push(PartOfSpeech::Noun);
                        part_of_speech.push(PartOfSpeech::Adjective);
                    },
                    x => {
                        word = x.to_string()
                    }
                }
            }
            (
                word,
                part_of_speech,
                level,
            )
        };
        if level == LangLevel::Unknown
            || part_of_speech.len() == 0 {
            println!(
                "Word: {:?}, POS: {:?}, Level: {:?}", word, part_of_speech, level
            );
            continue;
        }
        words.push(AWord {
            word,
            pos: part_of_speech,
            level,
        });
    }
    words
}

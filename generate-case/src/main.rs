use std::fs;
use std::env;

use env_logger;
use log::{debug};

const NUM_FILES:usize = 1;

fn main () {
    env_logger::try_init().expect("Can't initialize logger");

    let mut path = env::current_dir().unwrap();
    path.push("./generate-case/data/words_levels.txt");
    let file_path = path.canonicalize().unwrap();
    debug!("Reading from {:?}", file_path);

    match fs::read_to_string(file_path) {
        Ok(contents) => {
            let words = parse_file(&contents);
            for _ in 0..NUM_FILES {
                println!("{}", words.len());
            }
        },
        Err(e) => {
            println!("Error reading the file {:?}", e);
        }
    }
}

fn generate_file(words: &Vec<AWord>){

}

#[derive(Debug, PartialEq)]
enum PartOfSpeech {
    Verb,
    Noun,
    Adjective,
    Adverb,
    Preposition,
    Conjunction,
    Pronoun,
    Number,
    Determiner,
    Exclamation,
    Article,
    Unknown,
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
    pos: PartOfSpeech,
}

fn parse_file(contents: &String) -> Vec<AWord> {
    let mut words = Vec::new();
    let mut content_iter = contents.split("\n");
    while let (Some(line1), Some(line2)) = (content_iter.next(), content_iter.next()) {
        let (
            word,
            part_of_speech,
            level,
        ) = {
            let mut word: String = String::new();
            let mut part_of_speech: PartOfSpeech = PartOfSpeech::Unknown;
            let level: LangLevel = match line2.trim() {
                "a1" => {
                    LangLevel::A1
                },
                "a2" => {
                    LangLevel::A2
                },
                "b1" => {
                    LangLevel::B1
                },
                "b2" => {
                    LangLevel::B2
                },
                "c1" => {
                    LangLevel::C1
                },
                "c2" => {
                    LangLevel::C2
                },
                _ => LangLevel::Unknown
            };
            for part in line1.trim().split(" ").collect::<Vec<&str>>().iter().rev() {
                match (*part).trim_end_matches(',') {

                    "verb" => {
                        part_of_speech = PartOfSpeech::Verb
                    },
                    "noun" => {
                        part_of_speech = PartOfSpeech::Noun
                    },
                    "adjective" => {
                        part_of_speech = PartOfSpeech::Adjective
                    },
                    "adverb" => {
                        part_of_speech = PartOfSpeech::Adverb
                    },
                    "preposition" => {
                        part_of_speech = PartOfSpeech::Preposition
                    },
                    "conjunction" => {
                        part_of_speech = PartOfSpeech::Conjunction
                    },
                    "pronoun" => {
                        part_of_speech = PartOfSpeech::Pronoun
                    },
                    "number" => {
                        part_of_speech = PartOfSpeech::Number
                    },
                    "determiner" => {
                        part_of_speech = PartOfSpeech::Determiner
                    },
                    "exclamation" => {
                        part_of_speech = PartOfSpeech::Exclamation
                    },
                    "article" => {
                        part_of_speech = PartOfSpeech::Article
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
            || part_of_speech == PartOfSpeech::Unknown {
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

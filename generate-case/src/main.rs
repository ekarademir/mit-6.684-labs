use std::fs;
use std::io::prelude::*;
use std::env;
use std::hash::Hash;
use std::collections::HashMap;
use std::fmt;

use env_logger;
use log::{debug, info, error, warn};
use rand::thread_rng;
use rand::distributions::WeightedIndex;
use rand::prelude::*;

const NUM_FILES:usize = 3;
const WORD_COUNT_MIN:usize = 3;
const WORD_COUNT_MAX:usize = 10;
const SENTENCE_COUNT_MIN:usize = 7;
const SENTENCE_COUNT_MAX:usize = 50;
const PARAGRAPH_COUNT:usize = 20;

trait Capitalize {
    fn capitalize(&self) -> String;
}

impl Capitalize for String {
    fn capitalize(&self) -> String {
        let mut chars = self.chars();
        if let Some(first) = chars.next() {
            first.to_uppercase().collect::<String>() + chars.as_str()
        } else {
            String::new()
        }
    }
}

fn main () {
    env_logger::try_init().expect("Can't initialize logger");

    let mut path = env::current_dir().unwrap();
    path.push("./generate-case/data/words_levels.txt");
    let file_path = path.canonicalize().unwrap();
    debug!("Reading from {:?}", file_path);

    match fs::read_to_string(file_path) {
        Ok(contents) => {
            let words = parse_file(&contents);
            info!("There are {} words to generate from.", words.len());

            let mut word_bags: HashMap<PartOfSpeech, Vec<AWord>> = HashMap::new();
            for word in words {
                if let Some(bag) = word_bags.get_mut(&word.pos) {
                    bag.push(word);
                } else {
                    let mut new_bag: Vec<AWord> = Vec::new();
                    let key = word.pos.clone();
                    new_bag.push(word);
                    word_bags.insert(key, new_bag);
                }
            }

            for x in 0..NUM_FILES {
                info!("Generating file {} of {}", x+1, NUM_FILES);
                let mut path = env::current_dir().unwrap();
                path.push(
                    format!("./generate-case/output/generated_{:02}.txt", x)
                );
                generate_file(
                    &word_bags, path.to_str().unwrap()
                );
            }
        },
        Err(e) => {
            error!("Error reading the file {:?}", e);
        }
    }
}


#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
    pos: PartOfSpeech,
    #[allow(dead_code)]
    level: LangLevel,
}

impl fmt::Display for AWord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.word)
    }
}

fn generate_file(word_bags: &HashMap<PartOfSpeech, Vec<AWord>>, file_path: &str){
    let choices = [
        PartOfSpeech::Verb,
        PartOfSpeech::Noun,
        PartOfSpeech::Adjective,
        PartOfSpeech::Adverb,
        PartOfSpeech::Preposition,
        PartOfSpeech::Conjunction,
        PartOfSpeech::Pronoun,
        PartOfSpeech::Number,
        PartOfSpeech::Determiner,
        PartOfSpeech::Exclamation,
        PartOfSpeech::Article,
    ];
    let mut weights = [1; 11];
    weights[0] = 3;
    weights[1] = 3;
    weights[2] = 2;

    let dist = WeightedIndex::new(&weights).unwrap();
    let mut rng = thread_rng();

    let mut content: Vec<String> = Vec::new();

    for _ in 0..PARAGRAPH_COUNT {
        let mut paragraph: Vec<String> = Vec::new();
        for _ in 0..rng.gen_range(SENTENCE_COUNT_MIN, SENTENCE_COUNT_MAX) {
            let mut sentence: Vec<String> = Vec::new();
            for _ in 0..rng.gen_range(WORD_COUNT_MIN, WORD_COUNT_MAX) {
                let pos = choices[dist.sample(&mut rng)];
                let words = word_bags.get(&pos).unwrap();

                sentence.push(format!("{}", words[rng.gen_range(0, words.len())]));
            }
            sentence[0] = sentence[0].capitalize();
            paragraph.push(format!("{}.", sentence.join(" ")));
        }
        content.push(format!("{}", paragraph.join(" ")));
    }

    let contents = content.join("\n");

    // println!("{}", contents);
    debug!("Writing {:?}", file_path);
    let mut file = fs::File::create(
        file_path
    ).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    // TODO: Add weights for punctuations
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
                match *part {
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
                        if x.len() != 0 {
                            word = x.to_lowercase().to_string()
                        }
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
            || part_of_speech == PartOfSpeech::Unknown
            || word.len() == 0 {
            warn!(
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

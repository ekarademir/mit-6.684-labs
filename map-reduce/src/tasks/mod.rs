use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use serde::{Deserialize, Serialize};

// pub trait Mapper {
//     fn emitIntermediate(key: &str, value: String) -> Result<(), &str> {
//         println!("Emitting key: {}, value: {}", key, value);
//         Ok(())
//     }
// }

pub struct CompletedTask {
    key: String,
    value: String,
}

pub enum TaskError {
    SomeError,
}

type FutureTask = Pin<Box<dyn Future<Output = Result<CompletedTask, TaskError>> + Send>>;

pub trait MapReduceTask {
    fn execute(&self, input: String) -> FutureTask;
}

#[derive(Serialize, Deserialize)]
pub struct CountWords {
    input: String,
}

impl MapReduceTask for CountWords {
    // type Input = String;

    fn execute(&self, input: String) -> FutureTask {
        Box::pin(async {
            Ok(CompletedTask {
                key: "SomeKey".to_string(),
                value: "Some Value".to_string()
            })
        })
    }
}

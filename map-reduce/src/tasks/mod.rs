use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Queued,
    Finished,
    Failed,
    Started,
    Running,
    Error,
    CantAssign,
}

pub struct CompletedTask {
    key: String,
    value: String,
}

#[derive(PartialEq, Serialize, Deserialize)]
pub struct TaskInput {
    pub machine_addr: String,
    pub file: String,
}

pub enum TaskError {
    SomeError,
}

type FutureTask = Pin<Box<dyn Future<Output = Result<CompletedTask, TaskError>> + Send>>;

pub trait MapReduceTask {
    fn execute(&self) -> FutureTask;
}

#[derive(PartialEq, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: ATask,
    pub input: TaskInput,
}

// impl MapReduceTask for CountWords {
//     fn execute(&self) -> FutureTask {
//         Box::pin(async {
//             Ok(CompletedTask {
//                 key: "SomeKey".to_string(),
//                 value: "Some Value".to_string()
//             })
//         })
//     }
// }

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ATask {
    CountWords,
}

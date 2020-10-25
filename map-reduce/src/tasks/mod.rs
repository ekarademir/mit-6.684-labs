use log::debug;
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskInput {
    pub machine_addr: String,
    pub file: String,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: ATask,
    pub input: TaskInput,
}

// TODO Probably a macro to register a task which,
//    * Copies the function
//      * Either converts function_case to CamelCase or gets also ATask name
//    * Adds an entry in the match assignment

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ATask {
    CountWords,
}

impl TaskAssignment {
    pub async fn execute(&self) {
        match self.task {
            ATask::CountWords => count_words().await
        }
    }
}

pub async fn count_words() {
    debug!("Counting words!!!!");
}

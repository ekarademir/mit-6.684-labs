use log::debug;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskInput {
    pub machine_addr: String,
    pub file: String,
}

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ATask {
    CountWords,
    SumCounts,
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


impl TaskAssignment {
    pub async fn execute(&self) {
        match self.task {
            ATask::CountWords => count_words().await,
            ATask::SumCounts => sum_counts().await,
        }
    }
}

async fn count_words() {
    debug!("Counting words!!!!");
}

async fn sum_counts() {
    debug!("Total Count!!!!");
}

use log::debug;
use serde::{Deserialize, Serialize};

// TODO Probably a macro to register a task which,
//    * Copies the function
//      * Either converts function_case to CamelCase or gets also ATask name
//    * Adds an entry in the match assignment
// Tried this a bit but this is very hard to pull of as intended. So for not this is inconvenient
// but fast to implement


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskInput {
    pub machine_addr: String,
    pub file: String,
}

pub type TaskInputs = Vec<TaskInput>;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ATask {
    CountWords,
    SumCounts,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: ATask,
    pub input: TaskInputs,
}


impl TaskAssignment {
    pub async fn execute(&self) {
        // TODO Pull the contents of the input and feed them into the functions
        match self.task {
            ATask::CountWords => count_words(&self.input).await,
            ATask::SumCounts => sum_counts(&self.input).await,
        }
    }
}

async fn count_words(input: &TaskInputs) {
    debug!("Counting words!!!!");
}

async fn sum_counts(input: &TaskInputs) {
    debug!("Total Count!!!!");
}
